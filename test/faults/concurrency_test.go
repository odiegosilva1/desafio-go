//go:build integration

package faults

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"desafio-go/internal/application"
	"desafio-go/internal/auth"
	"desafio-go/internal/domain/money"
	"desafio-go/internal/domain/wagering"
	"desafio-go/internal/httpapi"
	"desafio-go/internal/messaging"
	"desafio-go/internal/observability"
)

// providerVerifier emula o OIDC: o Bearer "provider-a" autentica o provedor-a.
type providerVerifier struct{}

func (providerVerifier) Verify(_ context.Context, raw string) (auth.Principal, error) {
	if raw != "provider-a" {
		return auth.Principal{}, auth.ErrInvalidToken
	}
	return auth.Principal{Subject: "provider-a", ProviderID: "provider-a", Scopes: []string{auth.ScopeProvider}}, nil
}

// TestSameOperation50ParallelSingleDebit reproduz o specs §13.1: a MESMA aposta
// enviada 50 vezes em paralelo, com pools de conexões e memória independentes
// (instâncias distintas), deve produzir exatamente UM débito.
func TestSameOperation50ParallelSingleDebit(t *testing.T) {
	pool := newPool(t)
	resetSchema(t, pool)
	base := newService(t, pool)
	wid := openWalletFor(t, base, "provider-a", "player-50", "100.00")

	const total = 50
	const instances = 5
	pools := make([]*pgxpool.Pool, instances)
	svcs := make([]*application.Service, instances)
	t.Cleanup(func() {
		for _, p := range pools {
			p.Close()
		}
	})
	for i := 0; i < instances; i++ {
		pools[i] = newPool(t)
		svcs[i] = newService(t, pools[i])
	}

	amount := mustMoney(t, "25.00")
	in := application.ProcessInput{
		ProviderID:     "provider-a",
		ExternalTxID:   "tx-50",
		PlayerID:       "player-50",
		WalletID:       wid,
		RoundID:        "r1",
		GameID:         "g1",
		Kind:           wagering.KindBet,
		Money:          amount,
		IdempotencyKey: "provider-a:tx-50",
		OccurredAt:     time.Now().UTC(),
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	errs := make(chan error, total)
	replays := make(chan bool, total)
	for i := 0; i < total; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			<-start
			res, err := svcs[n%instances].Process(context.Background(), in)
			if err != nil {
				errs <- err
				return
			}
			replays <- res.IdempotentReplay
		}(i)
	}
	close(start)
	wg.Wait()
	close(errs)
	close(replays)

	for err := range errs {
		t.Errorf("process: %v", err)
	}
	applied, dups := 0, 0
	for r := range replays {
		if r {
			dups++
		} else {
			applied++
		}
	}
	if applied != 1 {
		t.Errorf("esperada exatamente UMA aplicação inicial, got %d", applied)
	}
	if dups != total-1 {
		t.Errorf("esperados %d replays idempotentes, got %d", total-1, dups)
	}
	if got := ledgerWagerCount(t, pool, wid); got != 1 {
		t.Errorf("débitos no ledger = %d, want 1", got)
	}
	if got := balance(t, pool, wid); got != "75.00" {
		t.Errorf("saldo = %s, want 75.00 (100.00 - 25.00, um único débito)", got)
	}
}

// TestHTTPAndSQSShareIdempotency cobre o specs §13 (final): a MESMA operação
// cruzando HTTP e SQS. A primeira via (HTTP) movimenta uma única vez; a entrada
// por SQS com a mesma chave/conteúdo é deduplicada (replay) — nunca um segundo
// débito.
func TestHTTPAndSQSShareIdempotency(t *testing.T) {
	pool := newPool(t)
	resetSchema(t, pool)
	svc := newService(t, pool)
	wid := openWalletFor(t, svc, "provider-a", "player-x", "100.00")

	logger := observability.NewLogger("error")
	metrics := observability.NewMetrics()
	router := httpapi.Router{
		Handlers: httpapi.NewHandlers(svc, logger, metrics),
		Verifier: providerVerifier{},
		Logger:   logger,
		Metrics:  metrics,
		Version:  "test",
	}.Handler()

	// --- 1) Entrada HTTP ---------------------------------------------------
	body := `{"providerId":"provider-a","externalTransactionId":"tx-x","playerId":"player-x",` +
		`"walletId":"` + wid + `","roundId":"r1","gameId":"g1","kind":"BET",` +
		`"money":{"amount":"25.00","currency":"BRL"}}`
	req := httptest.NewRequest(http.MethodPost, "/wagering/transactions", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer provider-a")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", "provider-a:tx-x")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("http submit = %d: %s", rec.Code, rec.Body.String())
	}
	var httpRes struct {
		Status           string `json:"status"`
		IdempotentReplay bool   `json:"idempotentReplay"`
		Balance          struct {
			Amount   string `json:"amount"`
			Currency string `json:"currency"`
		} `json:"balance"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &httpRes); err != nil {
		t.Fatalf("decode http response: %v", err)
	}
	if httpRes.Status != string(wagering.StateProcessed) || httpRes.IdempotentReplay {
		t.Errorf("http: status=%s replay=%v, want PROCESSED/false", httpRes.Status, httpRes.IdempotentReplay)
	}
	if httpRes.Balance.Amount != "75.00" {
		t.Errorf("http balance = %s, want 75.00", httpRes.Balance.Amount)
	}
	if got := ledgerWagerCount(t, pool, wid); got != 1 {
		t.Fatalf("ledger após HTTP = %d, want 1", got)
	}

	// --- 2) Mesma operação via SQS ----------------------------------------
	f := newFakeSQS("input", "dlq", "events")
	envBody, err := envelopeFor("msg-x", "provider-a", wid, "tx-x", "provider-a:tx-x", "player-x", "25.00")
	if err != nil {
		t.Fatalf("envelope: %v", err)
	}
	f.enqueue(message("msg-x", envBody))

	consumer := newConsumer(t, f, pool)
	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := runConsumer(runCtx, consumer)

	waitFor(t, 10*time.Second, func() bool { return f.deleteCount() == 1 })
	cancel()
	<-done

	if got := ledgerWagerCount(t, pool, wid); got != 1 {
		t.Errorf("ledger após SQS = %d, want 1 (sem débito duplicado)", got)
	}
	if got := balance(t, pool, wid); got != "75.00" {
		t.Errorf("saldo após SQS = %s, want 75.00", got)
	}
	if f.dlqCount() != 0 {
		t.Errorf("mensagem equivalente foi à DLQ (%d envios)", f.dlqCount())
	}
	inbox, ok := getInbox(t, pool, "msg-x")
	if !ok || inbox.Status != "PROCESSED" {
		t.Errorf("inbox msg-x = %+v, want PROCESSED", inbox)
	}
}

// openWalletFor abre uma carteira indicando o provedor (o helper openWallet
// fixa "p1").
func openWalletFor(t *testing.T, svc *application.Service, providerID, playerID, initial string) string {
	t.Helper()
	bal, err := money.FromDecimalString(initial, money.CurrencyBRL)
	if err != nil {
		t.Fatalf("parse initial balance: %v", err)
	}
	res, err := svc.OpenWallet(context.Background(), application.OpenWalletInput{
		ProviderID:     providerID,
		PlayerID:       playerID,
		InitialBalance: bal,
		OccurredAt:     time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("open wallet: %v", err)
	}
	return res.ID
}

// envelopeFor serializa um envelope BET no wire canônico, com provedor e chave
// de idempotência explícitos (equivale à entrada HTTP para a mesma operação).
func envelopeFor(messageID, providerID, walletID, externalTxID, idempotencyKey, playerID, amount string) (string, error) {
	m, err := messaging.MarshalInbound(messaging.InboundEnvelope{
		MessageID:  messageID,
		Type:       "WAGER",
		OccurredAt: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
		Data: messaging.InboundWagerData{
			ProviderID:            providerID,
			ExternalTransactionID: externalTxID,
			IdempotencyKey:        idempotencyKey,
			PlayerID:              playerID,
			WalletID:              walletID,
			RoundID:               "r1",
			GameID:                "g1",
			Kind:                  "BET",
			Money:                 messaging.MoneyJSON{Amount: amount, Currency: "BRL"},
		},
	})
	if err != nil {
		return "", err
	}
	return string(m), nil
}
