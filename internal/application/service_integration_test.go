//go:build integration

package application

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"desafio-go/internal/domain/derr"
	"desafio-go/internal/domain/money"
	"desafio-go/internal/domain/wagering"
	"desafio-go/internal/observability"
	"desafio-go/internal/storage/postgres"
)

// integrationPool cria um pool de conexões independente (simula um processo
// separado: conexões e memória próprias).
func integrationPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		url = "postgres://wallet:wallet@localhost:5432/wallet?sslmode=disable"
	}
	pool, err := postgres.NewPool(ctx, url)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// resetSchema zera o banco aplicando Down até a versão 0 e Up novamente.
func resetSchema(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	mig := postgres.NewMigrator(pool, os.DirFS("../app/migrations"))
	for {
		v, err := mig.Down(ctx)
		if err != nil {
			t.Fatalf("down cleanup: %v", err)
		}
		if v == 0 {
			break
		}
	}
	if err := mig.Up(ctx); err != nil {
		t.Fatalf("up: %v", err)
	}
}

// newServiceOnPool monta um Service sobre o pool informado, sem resetar o banco.
func newServiceOnPool(t *testing.T, pool *pgxpool.Pool) *Service {
	t.Helper()
	repos := Repos{
		Wallets:  postgres.NewWalletStore(),
		Ledger:   postgres.NewLedgerStore(),
		Wagering: postgres.NewWageringStore(),
		Outbox:   postgres.NewOutboxStore(),
		Inbox:    postgres.NewInboxStore(),
		UOW:      postgres.NewUnitOfWork(pool),
	}
	logger := observability.NewLogger("error")
	return NewService(repos, logger, observability.NewMetrics())
}

// integrationService prepara um Service sobre um banco zerado.
func integrationService(t *testing.T) (*Service, *pgxpool.Pool) {
	t.Helper()
	pool := integrationPool(t)
	resetSchema(t, pool)
	return newServiceOnPool(t, pool), pool
}

func input(in ProcessInput) ProcessInput {
	if in.OccurredAt.IsZero() {
		in.OccurredAt = time.Now().UTC()
	}
	if in.IdempotencyKey == "" {
		in.IdempotencyKey = in.ProviderID + ":" + in.ExternalTxID
	}
	return in
}

func TestOpenWalletProcessReplayAndRejectIntegration(t *testing.T) {
	svc, _ := integrationService(t)
	ctx := context.Background()

	bal, _ := money.FromDecimalString("1000.00", money.CurrencyBRL)
	opened, err := svc.OpenWallet(ctx, OpenWalletInput{
		ProviderID: "provider-a", PlayerID: "p1", InitialBalance: bal, OccurredAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if opened.Balance.Amount() != "1000.00" || opened.Version != 1 {
		t.Fatalf("unexpected opening: %+v", opened)
	}

	bet, _ := money.FromDecimalString("25.00", money.CurrencyBRL)
	in := input(ProcessInput{
		ProviderID: "provider-a", ExternalTxID: "ext-1", PlayerID: "p1",
		WalletID: opened.ID, RoundID: "r1", GameID: "g1", Kind: wagering.KindBet, Money: bet,
	})
	res, err := svc.Process(ctx, in)
	if err != nil {
		t.Fatalf("process bet: %v", err)
	}
	if res.Status != wagering.StateProcessed || res.Balance.Amount() != "975.00" {
		t.Fatalf("unexpected bet result: %+v", res)
	}

	// Replay idempotente não debita de novo.
	replay, err := svc.Process(ctx, in)
	if err != nil {
		t.Fatalf("process replay: %v", err)
	}
	if !replay.IdempotentReplay || replay.Balance.Amount() != "975.00" || replay.TransactionID != res.TransactionID {
		t.Fatalf("unexpected replay: %+v", replay)
	}

	// Winning credita.
	win, _ := money.FromDecimalString("250.00", money.CurrencyBRL)
	resWin, err := svc.Process(ctx, input(ProcessInput{
		ProviderID: "provider-a", ExternalTxID: "ext-2", PlayerID: "p1",
		WalletID: opened.ID, RoundID: "r1", GameID: "g1", Kind: wagering.KindWin, Money: win,
	}))
	if err != nil {
		t.Fatalf("process win: %v", err)
	}
	if resWin.Balance.Amount() != "1225.00" {
		t.Fatalf("unexpected win result: %+v", resWin)
	}

	// Aposta sem saldo → REJECTED definitivo, sem efeito financeiro.
	big, _ := money.FromDecimalString("999999.00", money.CurrencyBRL)
	resBig, err := svc.Process(ctx, input(ProcessInput{
		ProviderID: "provider-a", ExternalTxID: "ext-3", PlayerID: "p1",
		WalletID: opened.ID, RoundID: "r1", GameID: "g1", Kind: wagering.KindBet, Money: big,
	}))
	if err != nil {
		t.Fatalf("process big bet: %v", err)
	}
	if resBig.Status != wagering.StateRejected || resBig.FailureCode != derr.CodeInsufficientFunds {
		t.Fatalf("expected insufficient funds rejection, got %+v", resBig)
	}
	after, err := svc.GetWallet(ctx, "provider-a", opened.ID)
	if err != nil || after.Balance().Amount() != "1225.00" {
		t.Fatalf("balance should be unchanged after rejection: %+v err=%v", after, err)
	}

	// Reconciliação consistente com o ledger.
	rec, err := svc.Reconcile(ctx, opened.ID)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if !rec.Consistent || rec.CalculatedBalance.Amount() != "1225.00" {
		t.Fatalf("unexpected reconcile: %+v", rec)
	}
}

func TestPendingReferenceResolvedByWorkerIntegration(t *testing.T) {
	svc, _ := integrationService(t)
	ctx := context.Background()

	bal, _ := money.FromDecimalString("100.00", money.CurrencyBRL)
	opened, err := svc.OpenWallet(ctx, OpenWalletInput{
		ProviderID: "provider-a", PlayerID: "p1", InitialBalance: bal, OccurredAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	// REFUND antes do BET original (fora de ordem): entra em PENDING_REFERENCE.
	amt, _ := money.FromDecimalString("20.00", money.CurrencyBRL)
	res, err := svc.Process(ctx, input(ProcessInput{
		ProviderID: "provider-a", ExternalTxID: "refund-1", PlayerID: "p1",
		WalletID: opened.ID, RoundID: "r1", GameID: "g1", Kind: wagering.KindRefund,
		Money: amt, ReferenceExtID: "bet-orig",
	}))
	if err != nil {
		t.Fatalf("process refund: %v", err)
	}
	if res.Status != wagering.StatePendingReference {
		t.Fatalf("expected PENDING_REFERENCE, got %+v", res)
	}

	// Conferir que é possível consultar a transação pendente por id interno?
	// O worker retoma: primeiro vamos concluir o BET original.
	betRes, err := svc.Process(ctx, input(ProcessInput{
		ProviderID: "provider-a", ExternalTxID: "bet-orig", PlayerID: "p1",
		WalletID: opened.ID, RoundID: "r1", GameID: "g1", Kind: wagering.KindBet, Money: amt,
	}))
	if err != nil || betRes.Status != wagering.StateProcessed {
		t.Fatalf("process bet-orig: %v %+v", err, betRes)
	}

	// Worker resolve a referência pendente.
	worker := NewReferenceWorker(svc, 10*time.Millisecond)
	wctx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- worker.Run(wctx) }()

	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	var resolved bool
	for deadline := time.After(5 * time.Second); ; {
		balanced, err := svc.GetWallet(ctx, "provider-a", opened.ID)
		if err == nil && balanced.Balance().Amount() == "100.00" {
			resolved = true
			break
		}
		select {
		case <-deadline:
			cancel()
			<-done
			t.Fatalf("worker did not resolve pending reference, balance=%+v err=%v", balanced, err)
		case <-tick.C:
		}
	}
	cancel()
	<-done
	if !resolved {
		t.Fatal("pending reference not resolved")
	}
}
