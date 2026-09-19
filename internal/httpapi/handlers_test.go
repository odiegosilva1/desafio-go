package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"desafio-go/internal/application"
	"desafio-go/internal/auth"
	"desafio-go/internal/domain/derr"
	"desafio-go/internal/domain/ledger"
	"desafio-go/internal/domain/money"
	"desafio-go/internal/domain/wagering"
	"desafio-go/internal/domain/wallet"
	"desafio-go/internal/observability"
)

// tokenVerifier emite uma identidade conforme o "token" (provider-a,
// provider-b, internal) ou rejeita.
type tokenVerifier struct{}

func (tokenVerifier) Verify(_ context.Context, raw string) (auth.Principal, error) {
	switch raw {
	case "provider-a":
		return auth.Principal{Subject: "provider-a", ProviderID: "provider-a", Scopes: []string{auth.ScopeProvider}}, nil
	case "provider-b":
		return auth.Principal{Subject: "provider-b", ProviderID: "provider-b", Scopes: []string{auth.ScopeProvider}}, nil
	case "internal":
		return auth.Principal{Subject: "wallet-service-internal", IsInternal: true, Scopes: []string{auth.ScopeInternal, auth.ScopeAdmin}}, nil
	default:
		return auth.Principal{}, auth.ErrInvalidToken
	}
}

// stubService é a implementação de teste da interface Service.
type stubService struct {
	openWallet    func(ctx context.Context, in application.OpenWalletInput) (application.OpenWalletResult, error)
	process       func(ctx context.Context, in application.ProcessInput) (application.ProcessResult, error)
	getWallet     func(ctx context.Context, providerID, walletID string) (wallet.Wallet, error)
	listLedger    func(ctx context.Context, providerID, walletID string, afterSeq int64, limit int) ([]ledger.Entry, int64, error)
	getTx         func(ctx context.Context, transactionID string) (wagering.Transaction, error)
	getTxExternal func(ctx context.Context, providerID, externalTxID string) (wagering.Transaction, error)
	reconcile     func(ctx context.Context, walletID string) (*application.ReconciliationResult, error)
}

func (s *stubService) OpenWallet(ctx context.Context, in application.OpenWalletInput) (application.OpenWalletResult, error) {
	return s.openWallet(ctx, in)
}
func (s *stubService) Process(ctx context.Context, in application.ProcessInput) (application.ProcessResult, error) {
	return s.process(ctx, in)
}
func (s *stubService) GetWallet(ctx context.Context, providerID, walletID string) (wallet.Wallet, error) {
	return s.getWallet(ctx, providerID, walletID)
}
func (s *stubService) ListLedger(ctx context.Context, providerID, walletID string, afterSeq int64, limit int) ([]ledger.Entry, int64, error) {
	return s.listLedger(ctx, providerID, walletID, afterSeq, limit)
}
func (s *stubService) GetTransaction(ctx context.Context, transactionID string) (wagering.Transaction, error) {
	return s.getTx(ctx, transactionID)
}
func (s *stubService) GetTransactionByExternal(ctx context.Context, providerID, externalTxID string) (wagering.Transaction, error) {
	return s.getTxExternal(ctx, providerID, externalTxID)
}
func (s *stubService) Reconcile(ctx context.Context, walletID string) (*application.ReconciliationResult, error) {
	return s.reconcile(ctx, walletID)
}

func newTestRouter(svc Service) http.Handler {
	logger := observability.NewLogger("error")
	return Router{
		Handlers: NewHandlers(svc, logger, observability.NewMetrics()),
		Verifier: tokenVerifier{},
		Logger:   logger,
		Metrics:  observability.NewMetrics(),
	}.Handler()
}

func doJSON(t *testing.T, h http.Handler, method, path, token, idemKey string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatalf("encode body: %v", err)
		}
	}
	req := httptest.NewRequest(method, path, &buf)
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if idemKey != "" {
		req.Header.Set("Idempotency-Key", idemKey)
	}
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	return rr
}

func TestSubmitTransactionProcessed(t *testing.T) {
	bal, _ := money.FromDecimalString("975.00", money.CurrencyBRL)
	betAmount, _ := money.FromDecimalString("25.00", money.CurrencyBRL)
	svc := &stubService{process: func(_ context.Context, _ application.ProcessInput) (application.ProcessResult, error) {
		return application.ProcessResult{
			TransactionID: "tx-1", Status: wagering.StateProcessed, Balance: bal,
		}, nil
	}}
	h := newTestRouter(svc)

	rr := doJSON(t, h, http.MethodPost, "/wagering/transactions", "provider-a", "k1", processRequest{
		ExternalTransactionID: "ext-1", PlayerID: "p1", WalletID: "w1",
		RoundID: "r1", GameID: "g1", Kind: "BET", Money: betAmount,
	})
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rr.Code, rr.Body.String())
	}
	var resp processResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.TransactionID != "tx-1" || resp.Status != wagering.StateProcessed || resp.Balance == nil {
		t.Fatalf("unexpected response: %+v", resp)
	}
}

func TestSubmitTransactionRequiresIdempotencyKey(t *testing.T) {
	svc := &stubService{process: func(_ context.Context, _ application.ProcessInput) (application.ProcessResult, error) {
		t.Fatal("process should not be called")
		return application.ProcessResult{}, nil
	}}
	h := newTestRouter(svc)
	req := httptest.NewRequest(http.MethodPost, "/wagering/transactions", bytes.NewReader([]byte(`{}`)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer provider-a")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rr.Code)
	}
}

func TestSubmitTransactionPendingReferenceReturns202(t *testing.T) {
	amt, _ := money.FromDecimalString("25.00", money.CurrencyBRL)
	svc := &stubService{process: func(_ context.Context, _ application.ProcessInput) (application.ProcessResult, error) {
		return application.ProcessResult{
			TransactionID: "tx-pend", Status: wagering.StatePendingReference,
		}, nil
	}}
	h := newTestRouter(svc)
	rr := doJSON(t, h, http.MethodPost, "/wagering/transactions", "provider-a", "k-pend", processRequest{
		ExternalTransactionID: "ext-1", PlayerID: "p1", WalletID: "w1",
		RoundID: "r1", GameID: "g1", Kind: "REFUND", Money: amt, ReferenceExternalTxID: "orig",
	})
	if rr.Code != http.StatusAccepted {
		t.Fatalf("status = %d, body=%s", rr.Code, rr.Body.String())
	}
}

func TestSubmitTransactionBusinessRejection422(t *testing.T) {
	amt, _ := money.FromDecimalString("25.00", money.CurrencyBRL)
	svc := &stubService{process: func(_ context.Context, _ application.ProcessInput) (application.ProcessResult, error) {
		return application.ProcessResult{}, derr.ErrInsufficientFunds
	}}
	h := newTestRouter(svc)
	rr := doJSON(t, h, http.MethodPost, "/wagering/transactions", "provider-a", "k-1", processRequest{
		ExternalTransactionID: "ext-1", PlayerID: "p1", WalletID: "w1", RoundID: "r1",
		GameID: "g1", Kind: "BET", Money: amt,
	})
	if rr.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422", rr.Code)
	}
}

func TestWalletsOpenAndQuery(t *testing.T) {
	bal, _ := money.FromDecimalString("0.00", money.CurrencyBRL)
	wl, _, err := wallet.Open("w1", "provider-a", "p1", money.CurrencyBRL, money.Zero(money.CurrencyBRL), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	svc := &stubService{
		openWallet: func(_ context.Context, _ application.OpenWalletInput) (application.OpenWalletResult, error) {
			return application.OpenWalletResult{ID: "w1", PlayerID: "p1", Balance: bal, Version: 1}, nil
		},
		getWallet: func(_ context.Context, providerID, walletID string) (wallet.Wallet, error) {
			if providerID != "provider-a" {
				t.Fatalf("providerID = %s", providerID)
			}
			return wl, nil
		},
		listLedger: func(_ context.Context, _ string, _ string, _ int64, _ int) ([]ledger.Entry, int64, error) {
			return []ledger.Entry{}, 0, nil
		},
	}
	h := newTestRouter(svc)

	rr := doJSON(t, h, http.MethodPost, "/wallets", "provider-a", "", openWalletRequest{PlayerID: "p1"})
	if rr.Code != http.StatusCreated {
		t.Fatalf("open status = %d, body=%s", rr.Code, rr.Body.String())
	}

	rr = doJSON(t, h, http.MethodGet, "/wallets/w1", "provider-a", "", nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("get status = %d, body=%s", rr.Code, rr.Body.String())
	}
}

func TestAuthRequired(t *testing.T) {
	h := newTestRouter(&stubService{})
	req := httptest.NewRequest(http.MethodGet, "/wallets/w1", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rr.Code)
	}
}

func TestProviderIsolationOnTransactionQuery(t *testing.T) {
	processedTx, err := buildProcessedTransaction("provider-b")
	if err != nil {
		t.Fatal(err)
	}
	svc := &stubService{getTx: func(_ context.Context, id string) (wagering.Transaction, error) {
		return processedTx, nil
	}}
	h := newTestRouter(svc)

	// provider-a consulta transação de provider-b → não encontrada.
	rr := doJSON(t, h, http.MethodGet, "/wagering/transactions/tx-b", "provider-a", "", nil)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rr.Code)
	}
	// provider-b vê a própria transação.
	rr = doJSON(t, h, http.MethodGet, "/wagering/transactions/tx-b", "provider-b", "", nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
}

func TestInternalEndpointRejectsProviderToken(t *testing.T) {
	svc := &stubService{reconcile: func(_ context.Context, _ string) (*application.ReconciliationResult, error) {
		return &application.ReconciliationResult{Consistent: true}, nil
	}}
	h := newTestRouter(svc)

	rr := doJSON(t, h, http.MethodPost, "/wallets/w1/reconciliation", "provider-a", "", nil)
	if rr.Code != http.StatusForbidden && rr.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 403/401", rr.Code)
	}

	rr = doJSON(t, h, http.MethodPost, "/wallets/w1/reconciliation", "internal", "", nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200, body=%s", rr.Code, rr.Body.String())
	}
}

func buildProcessedTransaction(provider string) (wagering.Transaction, error) {
	m, _ := money.FromDecimalString("25.00", money.CurrencyBRL)
	now := time.Now()
	processed := now
	return wagering.Rehydrate(wagering.RehydrateOptions{
		ID: "tx-b", ExternalTxID: "ext-b", ProviderID: provider, PlayerID: "p-b",
		WalletID: "w-b", RoundID: "r-b", GameID: "g-b", Kind: wagering.KindBet, Money: m,
		IdempotencyKey: "k", OccurredAt: now, RecordedAt: now, ProcessedAt: &processed,
		WalletVersion: 1, ResultBalance: money.Zero(money.CurrencyBRL),
		State: wagering.StateProcessed,
	})
}

var _ = money.CurrencyBRL
