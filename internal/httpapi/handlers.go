package httpapi

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"desafio-go/internal/application"
	"desafio-go/internal/auth"
	"desafio-go/internal/domain/derr"
	"desafio-go/internal/domain/ledger"
	"desafio-go/internal/domain/money"
	"desafio-go/internal/domain/wagering"
	"desafio-go/internal/domain/wallet"
	"desafio-go/internal/observability"
	"desafio-go/internal/storage/port"
)

// Service é o front-end dos casos de uso consumido pelos handlers.
// *application.Service implementa a interface; testes usam um stub.
type Service interface {
	OpenWallet(ctx context.Context, in application.OpenWalletInput) (application.OpenWalletResult, error)
	Process(ctx context.Context, in application.ProcessInput) (application.ProcessResult, error)
	GetWallet(ctx context.Context, providerID, walletID string) (wallet.Wallet, error)
	ListLedger(ctx context.Context, providerID, walletID string, afterSeq int64, limit int) ([]ledger.Entry, int64, error)
	GetTransaction(ctx context.Context, transactionID string) (wagering.Transaction, error)
	GetTransactionByExternal(ctx context.Context, providerID, externalTxID string) (wagering.Transaction, error)
	Reconcile(ctx context.Context, walletID string) (*application.ReconciliationResult, error)
}

// Handlers agrupa os handlers da API de negócio.
type Handlers struct {
	service Service
	logger  *slog.Logger
	metrics *observability.Metrics
}

// NewHandlers constrói os handlers com as dependências necessárias.
func NewHandlers(service Service, logger *slog.Logger, metrics *observability.Metrics) *Handlers {
	return &Handlers{service: service, logger: logger, metrics: metrics}
}

// providerFromPrincipal devolve o providerId autorizado ou rejeita provedores
// que não tenham identidade de provedor (serviço interno).
func providerFromPrincipal(p auth.Principal) (string, error) {
	if p.IsInternal || p.ProviderID == "" {
		return "", derr.Newf(derr.ClassInvalidInput, derr.CodeNotFound, "identidade não é de provedor")
	}
	return p.ProviderID, nil
}

// --- Carteiras ---

func (h *Handlers) openWallet(w http.ResponseWriter, r *http.Request) {
	principal, _ := PrincipalFrom(r.Context())
	providerID, err := providerFromPrincipal(principal)
	if err != nil {
		writeError(w, http.StatusForbidden, "FORBIDDEN", "operação requer identidade de provedor")
		return
	}

	var req openWalletRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, string(derr.CodeInvalidPayload), err.Error())
		return
	}

	initial := money.Zero(money.CurrencyBRL)
	if req.InitialBalance != nil {
		initial = *req.InitialBalance
	}

	res, err := h.service.OpenWallet(r.Context(), application.OpenWalletInput{
		ProviderID:     providerID,
		PlayerID:       req.PlayerID,
		InitialBalance: initial,
		CorrelationID:  observability.TraceFrom(r.Context()).CorrelationID,
		OccurredAt:     time.Now().UTC(),
	})
	if err != nil {
		h.writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, openWalletResponse{
		ID: res.ID, PlayerID: res.PlayerID, Balance: res.Balance, Version: res.Version,
	})
}

func (h *Handlers) getWallet(w http.ResponseWriter, r *http.Request) {
	principal, _ := PrincipalFrom(r.Context())
	providerID, err := providerFromPrincipal(principal)
	if err != nil {
		writeError(w, http.StatusForbidden, "FORBIDDEN", "operação requer identidade de provedor")
		return
	}
	walletID := r.PathValue("walletId")
	wl, err := h.service.GetWallet(r.Context(), providerID, walletID)
	if err != nil {
		h.writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, walletResponse{
		ID: wl.ID(), PlayerID: wl.PlayerID(), Balance: wl.Balance(), Version: wl.Version(),
	})
}

func (h *Handlers) getLedger(w http.ResponseWriter, r *http.Request) {
	principal, _ := PrincipalFrom(r.Context())
	providerID, err := providerFromPrincipal(principal)
	if err != nil {
		writeError(w, http.StatusForbidden, "FORBIDDEN", "operação requer identidade de provedor")
		return
	}
	walletID := r.PathValue("walletId")

	afterSeq, err := decodeCursor(r.URL.Query().Get("cursor"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_CURSOR", "cursor opaco inválido")
		return
	}
	limit := 50
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			writeError(w, http.StatusBadRequest, "INVALID_LIMIT", "limit deve ser inteiro positivo")
			return
		}
		limit = n
	}

	entries, cursor, err := h.service.ListLedger(r.Context(), providerID, walletID, afterSeq, limit)
	if err != nil {
		h.writeAppError(w, err)
		return
	}
	out := make([]ledgerEntryResponse, 0, len(entries))
	for _, e := range entries {
		out = append(out, ledgerEntryResponse{
			ID:            e.ID(),
			TransactionID: e.TransactionID(),
			Direction:     e.Direction(),
			Money:         e.Money(),
			BalanceBefore: e.BalanceBefore(),
			BalanceAfter:  e.BalanceAfter(),
			CreatedAt:     e.CreatedAt(),
		})
	}
	resp := ledgerPageResponse{Entries: out}
	if len(out) == limit {
		resp.NextCursor = encodeCursor(cursor)
	}
	writeJSON(w, http.StatusOK, resp)
}

// --- Operações ---

func (h *Handlers) submitTransaction(w http.ResponseWriter, r *http.Request) {
	principal, _ := PrincipalFrom(r.Context())
	providerID, err := providerFromPrincipal(principal)
	if err != nil {
		writeError(w, http.StatusForbidden, "FORBIDDEN", "operação requer identidade de provedor")
		return
	}

	idempotencyKey := r.Header.Get("Idempotency-Key")
	if idempotencyKey == "" {
		writeError(w, http.StatusBadRequest, string(derr.CodeMissingIdempotencyKey), "header Idempotency-Key obrigatório")
		return
	}

	var req processRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, string(derr.CodeInvalidPayload), err.Error())
		return
	}
	// O providerId é determinado pela identidade autenticada; nunca confiar no
	// corpo para autorização.
	req.ProviderID = providerID

	occurred := time.Now().UTC()
	res, err := h.service.Process(r.Context(), application.ProcessInput{
		ProviderID:     req.ProviderID,
		ExternalTxID:   req.ExternalTransactionID,
		PlayerID:       req.PlayerID,
		WalletID:       req.WalletID,
		RoundID:        req.RoundID,
		GameID:         req.GameID,
		Kind:           wagering.Kind(req.Kind),
		Money:          req.Money,
		ReferenceExtID: req.ReferenceExternalTxID,
		IdempotencyKey: idempotencyKey,
		CorrelationID:  observability.TraceFrom(r.Context()).CorrelationID,
		OccurredAt:     occurred,
	})
	if err != nil {
		h.writeAppError(w, err)
		return
	}

	resp := processResponse{
		TransactionID:    res.TransactionID,
		Status:           res.Status,
		IdempotentReplay: res.IdempotentReplay,
		FailureCode:      res.FailureCode,
		FailureMessage:   res.FailureMessage,
	}
	if res.Status == wagering.StateProcessed {
		b := res.Balance
		resp.Balance = &b
	}
	switch res.Status {
	case wagering.StatePendingReference:
		writeJSON(w, http.StatusAccepted, resp)
	case wagering.StateRejected:
		writeJSON(w, http.StatusUnprocessableEntity, resp)
	default:
		writeJSON(w, http.StatusOK, resp)
	}
}

func (h *Handlers) getTransactionByID(w http.ResponseWriter, r *http.Request) {
	principal, _ := PrincipalFrom(r.Context())
	providerID, err := providerFromPrincipal(principal)
	if err != nil {
		writeError(w, http.StatusForbidden, "FORBIDDEN", "operação requer identidade de provedor")
		return
	}
	transactionID := r.PathValue("transactionId")
	t, err := h.service.GetTransaction(r.Context(), transactionID)
	if err != nil {
		h.writeAppError(w, err)
		return
	}
	// Isolamento por provedor: consultas e replays apenas das próprias transações.
	if t.ProviderID() != providerID {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "transação não encontrada")
		return
	}
	writeJSON(w, http.StatusOK, toTransactionResponse(t))
}

func (h *Handlers) getTransactionByExternal(w http.ResponseWriter, r *http.Request) {
	principal, _ := PrincipalFrom(r.Context())
	providerID := r.PathValue("providerId")
	if providerID != principal.ProviderID || principal.IsInternal {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "transação não encontrada")
		return
	}
	externalID := r.PathValue("externalTransactionId")
	t, err := h.service.GetTransactionByExternal(r.Context(), providerID, externalID)
	if err != nil {
		h.writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toTransactionResponse(t))
}

// --- Reconciliação (serviço interno) ---

func (h *Handlers) reconcileWallet(w http.ResponseWriter, r *http.Request) {
	walletID := r.PathValue("walletId")
	res, err := h.service.Reconcile(r.Context(), walletID)
	if err != nil {
		h.writeAppError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, reconcileResponse{
		WalletID:          res.WalletID,
		StoredBalance:     res.StoredBalance,
		CalculatedBalance: res.CalculatedBalance,
		Difference:        res.Difference,
		Consistent:        res.Consistent,
		CheckedEntries:    res.CheckedEntries,
	})
}

// writeAppError mapeia erros de domínio nos códigos HTTP do contrato.
func (h *Handlers) writeAppError(w http.ResponseWriter, err error) {
	class, ok := derr.ClassOf(err)
	code := derr.CodeOf(err)
	if code == "" {
		code = "INTERNAL"
	}
	switch {
	case errors.Is(err, derr.ErrIdempotencyConflict):
		writeError(w, http.StatusConflict, code, err.Error())
	case errors.Is(err, port.ErrNotFound):
		writeError(w, http.StatusNotFound, "NOT_FOUND", err.Error())
	case !ok || class == derr.ClassInvalidInput:
		writeError(w, http.StatusBadRequest, code, err.Error())
	case class == derr.ClassBusinessRule:
		writeError(w, http.StatusUnprocessableEntity, code, err.Error())
	case class == derr.ClassConflict:
		writeError(w, http.StatusConflict, code, err.Error())
	case class == derr.ClassPendingReference:
		writeError(w, http.StatusAccepted, code, err.Error())
	default:
		writeError(w, http.StatusServiceUnavailable, code, err.Error())
	}
}

// decodeJSON decodifica o corpo rejeitando JSON corrupto.
func decodeJSON(r *http.Request, v any) error {
	dec := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return err
	}
	return nil
}

// decodeCursor decodifica o cursor opaco (base64 do seq do ledger).
func decodeCursor(cursor string) (int64, error) {
	if cursor == "" {
		return 0, nil
	}
	b, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return 0, err
	}
	seq, err := strconv.ParseInt(string(b), 10, 64)
	if err != nil {
		return 0, err
	}
	return seq, nil
}

func encodeCursor(seq int64) string {
	return base64.RawURLEncoding.EncodeToString([]byte(strconv.FormatInt(seq, 10)))
}

func toTransactionResponse(t wagering.Transaction) transactionResponse {
	var resultBal *money.Money
	if t.State() == wagering.StateProcessed {
		b := t.ResultBalance()
		resultBal = &b
	}
	return transactionResponse{
		TransactionID:                  t.ID(),
		ProviderID:                     t.ProviderID(),
		ExternalTransactionID:          t.ExternalID(),
		PlayerID:                       t.PlayerID(),
		WalletID:                       t.WalletID(),
		RoundID:                        t.RoundID(),
		GameID:                         t.GameID(),
		Kind:                           string(t.Kind()),
		Money:                          t.Money(),
		ReferenceExternalTransactionID: t.ReferenceExtID(),
		Status:                         t.State(),
		FailureCode:                    t.FailureCode(),
		FailureMessage:                 t.FailureMessage(),
		ProcessedAt:                    t.ProcessedAt(),
		WalletVersion:                  t.WalletVersion(),
		ResultBalance:                  resultBal,
	}
}
