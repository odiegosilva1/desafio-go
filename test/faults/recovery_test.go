//go:build integration

package faults

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"desafio-go/internal/application"
	"desafio-go/internal/domain/wagering"
)

// TestTwoEqualBetsOverBalance reproduz a competição obrigatória de §13: duas
// apostas de 80.00 disputando a mesma carteira que abre com 100.00. Como a
// leitura e o débito da carteira são uma única transação atômica (SELECT FOR
// UPDATE), exatamente uma é PROCESSED e a outra é REJECTED por saldo
// insuficiente — 1 lançamento e saldo final de 20.00.
func TestTwoEqualBetsOverBalance(t *testing.T) {
	pool := newPool(t)
	resetSchema(t, pool)
	base := newService(t, pool)
	wid := openWalletFor(t, base, "provider-a", "player-race", "100.00")

	const instances = 2
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

	amount := mustMoney(t, "80.00")
	bets := []application.ProcessInput{
		{
			ProviderID: "provider-a", ExternalTxID: "race-a", PlayerID: "player-race",
			WalletID: wid, RoundID: "r1", GameID: "g1", Kind: wagering.KindBet,
			Money: amount, IdempotencyKey: "provider-a:race-a", OccurredAt: time.Now().UTC(),
		},
		{
			ProviderID: "provider-a", ExternalTxID: "race-b", PlayerID: "player-race",
			WalletID: wid, RoundID: "r1", GameID: "g1", Kind: wagering.KindBet,
			Money: amount, IdempotencyKey: "provider-a:race-b", OccurredAt: time.Now().UTC(),
		},
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	results := make(chan application.ProcessResult, len(bets))
	for i, in := range bets {
		wg.Add(1)
		go func(i int, in application.ProcessInput) {
			defer wg.Done()
			<-start
			res, err := svcs[i%instances].Process(context.Background(), in)
			if err != nil {
				results <- application.ProcessResult{FailureCode: "ERROR:" + err.Error()}
				return
			}
			results <- res
		}(i, in)
	}
	close(start)
	wg.Wait()
	close(results)

	processed, rejected := 0, 0
	for r := range results {
		switch {
		case r.Status == wagering.StateProcessed:
			processed++
		case r.Status == wagering.StateRejected && r.FailureCode == "INSUFFICIENT_FUNDS":
			rejected++
		case r.FailureCode != "":
			t.Errorf("erro inesperado: %s", r.FailureCode)
		default:
			t.Errorf("estado inesperado: status=%s code=%s", r.Status, r.FailureCode)
		}
	}
	if processed != 1 || rejected != 1 {
		t.Fatalf("processadas=%d rejeitadas=%d, want 1 processada e 1 insuficiente", processed, rejected)
	}
	if got := ledgerWagerCount(t, pool, wid); got != 1 {
		t.Errorf("ledger = %d, want 1 débito", got)
	}
	if got := balance(t, pool, wid); got != "20.00" {
		t.Errorf("saldo = %s, want 20.00 (100.00 - 80.00)", got)
	}
}

// TestConsumerCrashAfterCommitRedeliversOnce cobre §10: o consumidor morre
// após o commit do tratamento e ANTES de remover a mensagem da fila. A mesma
// mensagem é reentregue pelo broker e a reentrega é deduplicada pela inbox —
// nunca um segundo débito.
func TestConsumerCrashAfterCommitRedeliversOnce(t *testing.T) {
	pool := newPool(t)
	resetSchema(t, pool)
	svc := newService(t, pool)
	wallet := openWallet(t, svc, "player-crash", "100.00")

	body, err := betEnvelope("msg-crash", wallet, "tx-crash", "25.00")
	if err != nil {
		t.Fatalf("envelope: %v", err)
	}

	// Fase A — consumidor que comita o tratamento mas nunca remove a mensagem
	// (delete sempre em erro, simulando o crash no momento exato): a mensagem
	// permanece na fila para reentrega.
	crash := newFakeSQS("url/in", "url/dlq", "url/events")
	crash.deleteErr = errors.New("crash before delete")
	crash.enqueue(message("msg-crash", body))

	crashCtx, crashCancel := context.WithCancel(context.Background())
	crashDone := runConsumer(crashCtx, newConsumer(t, crash, pool))

	waitFor(t, 10*time.Second, func() bool { return ledgerWagerCount(t, pool, wallet) == 1 })
	if got := ledgerWagerCount(t, pool, wallet); got != 1 {
		t.Fatalf("após commit sem delete, ledger = %d, want 1", got)
	}
	row, ok := getInbox(t, pool, "msg-crash")
	if !ok || row.Status != "PROCESSED" {
		t.Fatalf("inbox = %+v, want PROCESSED (commit durável antes do delete)", row)
	}
	if got := crash.deleteCount(); got != 0 {
		t.Fatalf("delete não pode ter removido a mensagem, deletes=%d", got)
	}
	crashCancel()
	<-crashDone

	// Fase B — mesma fila, novo consumidor (a mensagem ainda está enfileirada):
	// a reentrega encontra a inbox PROCESSED e é deduplicada.
	recovered := newFakeSQS("url/in", "url/dlq", "url/events")
	recovered.enqueue(message("msg-crash", body))
	okCtx, okCancel := context.WithCancel(context.Background())
	okDone := runConsumer(okCtx, newConsumer(t, recovered, pool))
	waitFor(t, 10*time.Second, func() bool { return recovered.deleteCount() >= 1 })
	okCancel()
	<-okDone

	if got := ledgerWagerCount(t, pool, wallet); got != 1 {
		t.Fatalf("reentrega deduplicada violada: ledger = %d, want 1", got)
	}
	if b := balance(t, pool, wallet); b != "75.00" {
		t.Fatalf("saldo = %s, want 75.00", b)
	}
	if got := recovered.dlqCount(); got != 0 {
		t.Fatalf("reentrega não deve ir à DLQ, dlq=%d", got)
	}
}
