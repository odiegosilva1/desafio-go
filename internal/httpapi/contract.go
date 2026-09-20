package httpapi

import (
	"encoding/json"
	"net/http"
	"time"

	"desafio-go/internal/domain/ledger"
	"desafio-go/internal/domain/money"
	"desafio-go/internal/domain/wagering"
)

// errorBody é a estrutura de erro do contrato HTTP.
type errorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type errorResponse struct {
	Error errorBody `json:"error"`
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(errorResponse{Error: errorBody{Code: code, Message: message}})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// --- Abertura de carteira ---

type openWalletRequest struct {
	PlayerID string `json:"playerId"`
	// InitialBalance opcional: nil significa saldo inicial zero.
	InitialBalance *money.Money `json:"initialBalance,omitempty"`
}

type openWalletResponse struct {
	ID       string      `json:"id"`
	PlayerID string      `json:"playerId"`
	Balance  money.Money `json:"balance"`
	Version  int         `json:"version"`
}

type walletResponse struct {
	ID       string      `json:"id"`
	PlayerID string      `json:"playerId"`
	Balance  money.Money `json:"balance"`
	Version  int         `json:"version"`
}

// --- Operação de aposta ---

type processRequest struct {
	ProviderID            string      `json:"providerId"`
	ExternalTransactionID string      `json:"externalTransactionId"`
	PlayerID              string      `json:"playerId"`
	WalletID              string      `json:"walletId"`
	RoundID               string      `json:"roundId"`
	GameID                string      `json:"gameId"`
	Kind                  string      `json:"kind"`
	Money                 money.Money `json:"money"`
	ReferenceExternalTxID string      `json:"referenceExternalTransactionId,omitempty"`
}

type processResponse struct {
	TransactionID    string         `json:"transactionId"`
	Status           wagering.State `json:"status"`
	Balance          *money.Money   `json:"balance,omitempty"`
	IdempotentReplay bool           `json:"idempotentReplay"`
	FailureCode      string         `json:"failureCode,omitempty"`
	FailureMessage   string         `json:"failureMessage,omitempty"`
}

// --- Ledger ---

type ledgerEntryResponse struct {
	ID            string           `json:"id"`
	TransactionID string           `json:"transactionId"`
	Direction     ledger.Direction `json:"direction"`
	Money         money.Money      `json:"money"`
	BalanceBefore money.Money      `json:"balanceBefore"`
	BalanceAfter  money.Money      `json:"balanceAfter"`
	CreatedAt     time.Time        `json:"createdAt"`
}

type ledgerPageResponse struct {
	Entries    []ledgerEntryResponse `json:"entries"`
	NextCursor string                `json:"nextCursor,omitempty"`
}

// --- Reconciliação ---

type reconcileResponse struct {
	WalletID          string      `json:"walletId"`
	StoredBalance     money.Money `json:"storedBalance"`
	CalculatedBalance money.Money `json:"calculatedBalance"`
	Difference        money.Money `json:"difference"`
	Consistent        bool        `json:"consistent"`
	CheckedEntries    int         `json:"checkedEntries"`
}

// --- Transações (consulta) ---

type transactionResponse struct {
	TransactionID                  string         `json:"transactionId"`
	ProviderID                     string         `json:"providerId,omitempty"`
	ExternalTransactionID          string         `json:"externalTransactionId,omitempty"`
	PlayerID                       string         `json:"playerId"`
	WalletID                       string         `json:"walletId"`
	RoundID                        string         `json:"roundId,omitempty"`
	GameID                         string         `json:"gameId,omitempty"`
	Kind                           string         `json:"kind"`
	Money                          money.Money    `json:"money"`
	ReferenceExternalTransactionID string         `json:"referenceExternalTransactionId,omitempty"`
	Status                         wagering.State `json:"status"`
	FailureCode                    string         `json:"failureCode,omitempty"`
	FailureMessage                 string         `json:"failureMessage,omitempty"`
	ProcessedAt                    *time.Time     `json:"processedAt,omitempty"`
	WalletVersion                  int            `json:"walletVersion,omitempty"`
	ResultBalance                  *money.Money   `json:"resultBalance,omitempty"`
}

// --- Health ---

type healthResponse struct {
	Status  string            `json:"status"`
	Checks  map[string]string `json:"checks,omitempty"`
	Service string            `json:"service"`
	Time    time.Time         `json:"time"`
}
