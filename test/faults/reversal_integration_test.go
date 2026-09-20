//go:build integration

package faults

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"desafio-go/internal/application"
	"desafio-go/internal/domain/derr"
	"desafio-go/internal/domain/money"
	"desafio-go/internal/domain/wagering"
)

// wagerState devolve estado e failure_code persistidos de uma transação.
func wagerState(t *testing.T, pool *pgxpool.Pool, providerID, externalTxID string) (state, failureCode string) {
	t.Helper()
	err := pool.QueryRow(context.Background(),
		`SELECT state, coalesce(failure_code,'') FROM wagering_transactions WHERE provider_id=$1 AND external_tx_id=$2`,
		providerID, externalTxID).Scan(&state, &failureCode)
	if err != nil {
		t.Fatalf("wager state: %v", err)
	}
	return state, failureCode
}

// runReferenceWorker desemparelha o worker de referências pendentes.
func runReferenceWorker(ctx context.Context, svc *application.Service, interval time.Duration) chan struct{} {
	done := make(chan struct{})
	go func() { _ = application.NewReferenceWorker(svc, interval).Run(ctx); close(done) }()
	return done
}

// TestRefundThenRollbackSameBetRejected é a regressão central de §7: uma mesma
// BET nunca recebe duas reversões bem-sucedidas que devolvam o débito. O
// ROLLBACK posterior encontra a REFUND já processada sobre a referência —
// mesmo efeito CREDIT — e é rejeitado com DUPLICATE_REVERSAL, sem segundo
// crédito.
func TestRefundThenRollbackSameBetRejected(t *testing.T) {
	pool := newPool(t)
	resetSchema(t, pool)
	svc := newService(t, pool)
	wid := openWalletFor(t, svc, "provider-a", "player-1", "100.00")

	betInput := application.ProcessInput{
		ProviderID: "provider-a", ExternalTxID: "bet-1", PlayerID: "player-1",
		WalletID: wid, RoundID: "r1", GameID: "g1", Kind: wagering.KindBet,
		Money: mustMoney(t, "25.00"), IdempotencyKey: "provider-a:bet-1",
		OccurredAt: time.Now().UTC(),
	}
	if res, err := svc.Process(context.Background(), betInput); err != nil || res.Status != wagering.StateProcessed {
		t.Fatalf("bet: status=%s err=%v", res.Status, err)
	}
	if got := balance(t, pool, wid); got != "75.00" {
		t.Fatalf("saldo pós-BET = %s, want 75.00", got)
	}

	refund := application.ProcessInput{
		ProviderID: "provider-a", ExternalTxID: "refund-1", PlayerID: "player-1",
		WalletID: wid, RoundID: "r1", GameID: "g1", Kind: wagering.KindRefund,
		Money: mustMoney(t, "25.00"), ReferenceExtID: "bet-1",
		IdempotencyKey: "provider-a:refund-1", OccurredAt: time.Now().UTC(),
	}
	if res, err := svc.Process(context.Background(), refund); err != nil || res.Status != wagering.StateProcessed {
		t.Fatalf("refund: status=%s err=%v", res.Status, err)
	}
	if got := balance(t, pool, wid); got != "100.00" {
		t.Fatalf("saldo pós-REFUND = %s, want 100.00 (débito devolvido)", got)
	}

	rollback := application.ProcessInput{
		ProviderID: "provider-a", ExternalTxID: "rollback-1", PlayerID: "player-1",
		WalletID: wid, RoundID: "r1", GameID: "g1", Kind: wagering.KindRollback,
		Money: mustMoney(t, "25.00"), ReferenceExtID: "bet-1",
		IdempotencyKey: "provider-a:rollback-1", OccurredAt: time.Now().UTC(),
	}
	res, err := svc.Process(context.Background(), rollback)
	if err != nil {
		t.Fatalf("rollback deve concluir como rejeição de negócio, err=%v", err)
	}
	if res.Status != wagering.StateRejected || res.FailureCode != derr.CodeDuplicateReversal {
		t.Fatalf("rollback duplicado = %s/%s, want REJECTED/DUPLICATE_REVERSAL",
			res.Status, res.FailureCode)
	}
	if got := balance(t, pool, wid); got != "100.00" {
		t.Fatalf("saldo final = %s, want 100.00 (sem segundo crédito)", got)
	}
	if got := ledgerWagerCount(t, pool, wid); got != 2 {
		t.Fatalf("ledger = %d, want 2 (débito da BET + crédito da REFUND)", got)
	}
	state, code := wagerState(t, pool, "provider-a", "rollback-1")
	if state != string(wagering.StateRejected) || code != derr.CodeDuplicateReversal {
		t.Fatalf("rollback persistido = %s/%s", state, code)
	}
}

// TestCurrencyMismatchNoFinancialEffect cobre §6.2: uma operação em moeda
// diferente da carteira jamais movimenta saldo. O schema vigora BRL-only
// (CHECK currency IN ('BRL')) e rejeita a inserção; a validação de domínio
// (CURRENCY_MISMATCH) é a rede de segurança para a futura multiplicidade de
// moedas. Em qualquer das camadas, o efeito financeiro é zero.
func TestCurrencyMismatchNoFinancialEffect(t *testing.T) {
	pool := newPool(t)
	resetSchema(t, pool)
	svc := newService(t, pool)
	wid := openWalletFor(t, svc, "provider-a", "player-cur", "100.00")

	usd, err := money.FromDecimalString("25.00", "USD")
	if err != nil {
		t.Fatalf("usd: %v", err)
	}
	for _, kind := range []wagering.Kind{wagering.KindBet, wagering.KindWin, wagering.KindLoss} {
		name := string(kind)
		res, err := svc.Process(context.Background(), application.ProcessInput{
			ProviderID: "provider-a", ExternalTxID: "cur-" + name, PlayerID: "player-cur",
			WalletID: wid, RoundID: "r1", GameID: "g1", Kind: kind,
			Money: usd, IdempotencyKey: "provider-a:cur-" + name,
			OccurredAt: time.Now().UTC(),
		})
		if err == nil && res.Status == wagering.StateProcessed {
			t.Fatalf("%s: operação em moeda divergente não pode ser processada", name)
		}
		if err == nil && res.Status == wagering.StatePendingReference {
			t.Fatalf("%s: moeda divergente não pode entrar em espera", name)
		}
	}
	if got := ledgerWagerCount(t, pool, wid); got != 0 {
		t.Fatalf("moeda divergente não pode movimentar saldo, ledger=%d", got)
	}
	if got := balance(t, pool, wid); got != "100.00" {
		t.Fatalf("saldo = %s, want 100.00", got)
	}
}

// TestRefundRequiresProcessedBet cobre §7: a referência precisa ter sido
// processada com sucesso — REFUND/ROLLBACK sobre uma operação REJECTED/FAILED
// é rejeitado de forma definitiva (REFERENCE_NOT_PROCESSED).
func TestRefundRequiresProcessedBet(t *testing.T) {
	pool := newPool(t)
	resetSchema(t, pool)
	svc := newService(t, pool)
	wid := openWalletFor(t, svc, "provider-a", "player-ref", "10.00")

	// Aposta rejeitada por saldo insuficiente.
	if res, err := svc.Process(context.Background(), application.ProcessInput{
		ProviderID: "provider-a", ExternalTxID: "bet-fail", PlayerID: "player-ref",
		WalletID: wid, RoundID: "r1", GameID: "g1", Kind: wagering.KindBet,
		Money: mustMoney(t, "100.00"), IdempotencyKey: "provider-a:bet-fail",
		OccurredAt: time.Now().UTC(),
	}); err != nil || res.Status != wagering.StateRejected {
		t.Fatalf("bet-fail: status=%s err=%v", res.Status, err)
	}

	// REFUND sobre uma BET rejeitada jamais credita.
	res, err := svc.Process(context.Background(), application.ProcessInput{
		ProviderID: "provider-a", ExternalTxID: "refund-fail", PlayerID: "player-ref",
		WalletID: wid, RoundID: "r1", GameID: "g1", Kind: wagering.KindRefund,
		Money: mustMoney(t, "100.00"), ReferenceExtID: "bet-fail",
		IdempotencyKey: "provider-a:refund-fail", OccurredAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("refund sobre rejeitada: err=%v", err)
	}
	if res.Status != wagering.StateRejected || res.FailureCode != derr.CodeReferenceNotProcessed {
		t.Fatalf("refund-fail = %s/%s, want REJECTED/REFERENCE_NOT_PROCESSED",
			res.Status, res.FailureCode)
	}
	if got := balance(t, pool, wid); got != "10.00" {
		t.Fatalf("saldo = %s, want 10.00", got)
	}
	if got := ledgerWagerCount(t, pool, wid); got != 0 {
		t.Fatalf("nenhuma movimentação em rejeições, ledger=%d", got)
	}
}

// TestRollbackWinTwiceRejected verifica o segundo sentido de efeito (§7):
// um mesmo crédito de WIN não é desfeito duas vezes — o segundo ROLLBACK é
// rejeitado com DUPLICATE_REVERSAL (efeito DEBIT).
func TestRollbackWinTwiceRejected(t *testing.T) {
	pool := newPool(t)
	resetSchema(t, pool)
	svc := newService(t, pool)
	wid := openWalletFor(t, svc, "provider-a", "player-2", "100.00")

	process := func(id string, kind wagering.Kind, amt string, ref string) application.ProcessResult {
		t.Helper()
		res, err := svc.Process(context.Background(), application.ProcessInput{
			ProviderID: "provider-a", ExternalTxID: id, PlayerID: "player-2",
			WalletID: wid, RoundID: "r1", GameID: "g1", Kind: kind,
			Money: mustMoney(t, amt), ReferenceExtID: ref,
			IdempotencyKey: "provider-a:" + id, OccurredAt: time.Now().UTC(),
		})
		if err != nil {
			t.Fatalf("%s: err=%v", id, err)
		}
		return res
	}

	process("bet-w", wagering.KindBet, "25.00", "")
	process("win-w", wagering.KindWin, "25.00", "")
	if got := balance(t, pool, wid); got != "100.00" {
		t.Fatalf("saldo após BET+WIN = %s, want 100.00", got)
	}

	r1 := process("rb-w1", wagering.KindRollback, "25.00", "win-w")
	if r1.Status != wagering.StateProcessed || r1.FailureCode != "" {
		t.Fatalf("primeiro ROLLBACK = %s/%s, want PROCESSED", r1.Status, r1.FailureCode)
	}
	if got := balance(t, pool, wid); got != "75.00" {
		t.Fatalf("saldo após 1º ROLLBACK = %s, want 75.00", got)
	}

	r2 := process("rb-w2", wagering.KindRollback, "25.00", "win-w")
	if r2.Status != wagering.StateRejected || r2.FailureCode != derr.CodeDuplicateReversal {
		t.Fatalf("segundo ROLLBACK = %s/%s, want REJECTED/DUPLICATE_REVERSAL",
			r2.Status, r2.FailureCode)
	}
	if got := balance(t, pool, wid); got != "75.00" {
		t.Fatalf("saldo final = %s, want 75.00 (sem segundo débito)", got)
	}
}

// TestRollbackOutOfOrderResolvedByWorker cobre §9: o ROLLBACK enviado antes da
// referência entra em PENDING_REFERENCE (sem falha). Quando a BET chega, o
// worker resolve e credita a devolução — saldo volta ao total.
func TestRollbackOutOfOrderResolvedByWorker(t *testing.T) {
	pool := newPool(t)
	resetSchema(t, pool)
	svc := newService(t, pool)
	wid := openWalletFor(t, svc, "provider-a", "player-3", "100.00")

	// ROLLBACK para uma BET que ainda não existe.
	rollback, err := svc.Process(context.Background(), application.ProcessInput{
		ProviderID: "provider-a", ExternalTxID: "rb-early", PlayerID: "player-3",
		WalletID: wid, RoundID: "r1", GameID: "g1", Kind: wagering.KindRollback,
		Money: mustMoney(t, "40.00"), ReferenceExtID: "bet-late",
		IdempotencyKey: "provider-a:rb-early", OccurredAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("rollback inicial: %v", err)
	}
	if rollback.Status != wagering.StatePendingReference {
		t.Fatalf("status = %s, want PENDING_REFERENCE", rollback.Status)
	}
	if got := balance(t, pool, wid); got != "100.00" {
		t.Fatalf("saldo intocado antes da referência, got %s", got)
	}

	// A referência chega depois: BET de 40.00.
	if res, err := svc.Process(context.Background(), application.ProcessInput{
		ProviderID: "provider-a", ExternalTxID: "bet-late", PlayerID: "player-3",
		WalletID: wid, RoundID: "r1", GameID: "g1", Kind: wagering.KindBet,
		Money: mustMoney(t, "40.00"), IdempotencyKey: "provider-a:bet-late",
		OccurredAt: time.Now().UTC(),
	}); err != nil || res.Status != wagering.StateProcessed {
		t.Fatalf("bet: status=%s err=%v", res.Status, err)
	}
	if got := balance(t, pool, wid); got != "60.00" {
		t.Fatalf("saldo pós-BET = %s, want 60.00", got)
	}

	// O worker de referências resolve o ROLLBACK pendente: crédito de 40.00.
	ctx, cancel := context.WithCancel(context.Background())
	workerDone := runReferenceWorker(ctx, svc, 20*time.Millisecond)
	waitFor(t, 10*time.Second, func() bool {
		state, _ := wagerState(t, pool, "provider-a", "rb-early")
		return state == string(wagering.StateProcessed)
	})
	if got := balance(t, pool, wid); got != "100.00" {
		t.Fatalf("saldo após worker = %s, want 100.00 (devolução aplicada)", got)
	}
	cancel()
	<-workerDone
}

// TestPendingReferenceExpiresRejected cobre §9: uma referência que nunca chega
// esgota as tentativas do worker e a operação termina REJECTED com
// REFERENCE_NOT_FOUND, sem efeito financeiro.
func TestPendingReferenceExpiresRejected(t *testing.T) {
	pool := newPool(t)
	resetSchema(t, pool)
	svc := newService(t, pool)
	wid := openWalletFor(t, svc, "provider-a", "player-4", "100.00")

	if res, err := svc.Process(context.Background(), application.ProcessInput{
		ProviderID: "provider-a", ExternalTxID: "rb-never", PlayerID: "player-4",
		WalletID: wid, RoundID: "r1", GameID: "g1", Kind: wagering.KindRollback,
		Money: mustMoney(t, "15.00"), ReferenceExtID: "bet-never",
		IdempotencyKey: "provider-a:rb-never", OccurredAt: time.Now().UTC(),
	}); err != nil || res.Status != wagering.StatePendingReference {
		t.Fatalf("rollback inicial = %s/%v, want PENDING_REFERENCE", res.Status, err)
	}

	// Acelera o TTL: aproxima a operação do limite de tentativas para o worker
	// finalizar imediatamente (única tentativa restante).
	if _, err := pool.Exec(context.Background(),
		`UPDATE wagering_transactions SET attempts = 4, next_attempt_at = now()
		  WHERE provider_id='provider-a' AND external_tx_id='rb-never'`); err != nil {
		t.Fatalf("acelerar ttl: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	workerDone := runReferenceWorker(ctx, svc, 20*time.Millisecond)
	waitFor(t, 10*time.Second, func() bool {
		state, code := wagerState(t, pool, "provider-a", "rb-never")
		return state == string(wagering.StateRejected) && code == derr.CodeReferenceNotFound
	})
	cancel()
	<-workerDone

	if got := balance(t, pool, wid); got != "100.00" {
		t.Fatalf("expiração não pode movimentar saldo, got %s", got)
	}
	if got := ledgerWagerCount(t, pool, wid); got != 0 {
		t.Fatalf("nenhum efeito financeiro na expiração, ledger=%d", got)
	}
}
