//go:build integration

package faults

import (
	"context"
	"errors"
	"testing"
	"time"

	"desafio-go/internal/application"
	"desafio-go/internal/domain/derr"
	"desafio-go/internal/domain/event"
	"desafio-go/internal/domain/wagering"
)

// TestIdempotencyConflictDifferentPayload cobre §13: a MESMA chave de
// idempotência reutilizada com conteúdo financeiro diferente é um conflito
// persistente (IDEMPOTENCY_CONFLICT) — jamais um segundo processamento.
func TestIdempotencyConflictDifferentPayload(t *testing.T) {
	pool := newPool(t)
	resetSchema(t, pool)
	svc := newService(t, pool)
	wid := openWalletFor(t, svc, "provider-a", "player-5", "100.00")

	in := application.ProcessInput{
		ProviderID: "provider-a", ExternalTxID: "tx-key1", PlayerID: "player-5",
		WalletID: wid, RoundID: "r1", GameID: "g1", Kind: wagering.KindBet,
		Money: mustMoney(t, "25.00"), IdempotencyKey: "provider-a:key-1",
		OccurredAt: time.Now().UTC(),
	}
	if res, err := svc.Process(context.Background(), in); err != nil || res.Status != wagering.StateProcessed {
		t.Fatalf("1ª processamento: status=%s err=%v", res.Status, err)
	}

	// Mesma chave, mesmo external_tx_id, porém montante diferente.
	conflicting := in
	conflicting.Money = mustMoney(t, "30.00")
	if _, err := svc.Process(context.Background(), conflicting); !errors.Is(err, derr.ErrIdempotencyConflict) {
		t.Fatalf("conflito = %v, want ErrIdempotencyConflict", err)
	}
	if got := ledgerWagerCount(t, pool, wid); got != 1 {
		t.Fatalf("conflito não pode movimentar saldo, ledger=%d", got)
	}
	if got := balance(t, pool, wid); got != "75.00" {
		t.Fatalf("saldo = %s, want 75.00", got)
	}

	// Conteúdo idêntico continua replay idempotente.
	res, err := svc.Process(context.Background(), in)
	if err != nil || !res.IdempotentReplay {
		t.Fatalf("replay após conflito: res=%+v err=%v", res, err)
	}
	if got := ledgerWagerCount(t, pool, wid); got != 1 {
		t.Fatalf("replay duplicou, ledger=%d", got)
	}
}

// TestReplayAfterRestart simula a reinicialização do processo: uma nova
// instância sobre o mesmo banco (pools e memória descartados) reproduz o
// replay idempotente e retoma pendências de referência exatamente onde o
// processo anterior parou.
func TestReplayAfterRestart(t *testing.T) {
	pool := newPool(t)
	resetSchema(t, pool)

	// --- instância "1" ------------------------------------------------------
	svc1 := newService(t, pool)
	wid := openWalletFor(t, svc1, "provider-a", "player-6", "100.00")
	bet := application.ProcessInput{
		ProviderID: "provider-a", ExternalTxID: "bet-restart", PlayerID: "player-6",
		WalletID: wid, RoundID: "r1", GameID: "g1", Kind: wagering.KindBet,
		Money: mustMoney(t, "25.00"), IdempotencyKey: "provider-a:bet-restart",
		OccurredAt: time.Now().UTC(),
	}
	if res, err := svc1.Process(context.Background(), bet); err != nil || res.Status != wagering.StateProcessed {
		t.Fatalf("instância 1: status=%s err=%v", res.Status, err)
	}

	// ROLLBACK fora de ordem deixa o PENDING_REFERENCE na instância 1.
	if res, err := svc1.Process(context.Background(), application.ProcessInput{
		ProviderID: "provider-a", ExternalTxID: "rb-restart", PlayerID: "player-6",
		WalletID: wid, RoundID: "r1", GameID: "g1", Kind: wagering.KindRollback,
		Money: mustMoney(t, "10.00"), ReferenceExtID: "bet-restart-next",
		IdempotencyKey: "provider-a:rb-restart", OccurredAt: time.Now().UTC(),
	}); err != nil || res.Status != wagering.StatePendingReference {
		t.Fatalf("instância 1 rollback: status=%s err=%v", res.Status, err)
	}

	// --- "restart": instância 2, estado zero em memória ---------------------
	svc2 := newService(t, pool)
	res, err := svc2.Process(context.Background(), bet)
	if err != nil || !res.IdempotentReplay {
		t.Fatalf("replay após restart: res=%+v err=%v", res, err)
	}
	if got := ledgerWagerCount(t, pool, wid); got != 1 {
		t.Fatalf("restart não pode duplicar débito, ledger=%d", got)
	}

	// A referência chega e o worker da instância 2 resolve o pendente.
	if _, err := svc2.Process(context.Background(), application.ProcessInput{
		ProviderID: "provider-a", ExternalTxID: "bet-restart-next", PlayerID: "player-6",
		WalletID: wid, RoundID: "r1", GameID: "g1", Kind: wagering.KindBet,
		Money: mustMoney(t, "10.00"), IdempotencyKey: "provider-a:bet-restart-next",
		OccurredAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("instância 2 bet: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	workerDone := runReferenceWorker(ctx, svc2, 20*time.Millisecond)
	waitFor(t, 10*time.Second, func() bool {
		state, _ := wagerState(t, pool, "provider-a", "rb-restart")
		return state == string(wagering.StateProcessed)
	})
	cancel()
	<-workerDone

	if got := balance(t, pool, wid); got != "75.00" {
		t.Fatalf("saldo após restart = %s, want 75.00 (100.00 - 25.00 - 10.00 + 10.00)", got)
	}
}

// TestOpeningEmitsWalletEvents garante que a abertura de carteira publica os
// eventos esperados (WagerTransactionProcessed para o OPENING e
// WalletBalanceChanged) na outbox, no mesmo commit da abertura.
func TestOpeningEmitsWalletEvents(t *testing.T) {
	pool := newPool(t)
	resetSchema(t, pool)
	svc := newService(t, pool)

	wid := openWalletFor(t, svc, "provider-a", "player-7", "100.00")
	if wid == "" {
		t.Fatal("carteira vazia")
	}

	processed, balanceChanged := 0, 0
	rows, err := pool.Query(context.Background(),
		`SELECT event_type, aggregate_id FROM outbox WHERE status='PENDING' ORDER BY event_type`)
	if err != nil {
		t.Fatalf("outbox: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var et, agg string
		if err := rows.Scan(&et, &agg); err != nil {
			t.Fatal(err)
		}
		switch event.EventType(et) {
		case event.TypeWagerTransactionProcessed:
			processed++
		case event.TypeWalletBalanceChanged:
			balanceChanged++
			if agg != wid {
				t.Fatalf("balance aggregate = %q, want wallet %q", agg, wid)
			}
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if processed != 1 || balanceChanged != 1 {
		t.Fatalf("eventos da abertura = processed:%d balance:%d, want 1/1", processed, balanceChanged)
	}
}
