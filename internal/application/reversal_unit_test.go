package application

import (
	"errors"
	"testing"
	"time"

	"desafio-go/internal/domain/derr"
	"desafio-go/internal/domain/money"
	"desafio-go/internal/domain/wagering"
)

func mustMoneyT(t *testing.T, amount string) money.Money {
	t.Helper()
	m, err := money.FromDecimalString(amount, money.CurrencyBRL)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func pendingTx(t *testing.T, kind wagering.Kind, externalID, refExt string, amt money.Money) wagering.Transaction {
	t.Helper()
	tx, err := wagering.NewPending(basePendingOptions(kind, externalID, refExt, amt))
	if err != nil {
		t.Fatal(err)
	}
	return tx
}

func basePendingOptions(kind wagering.Kind, externalID, refExt string, amt money.Money) wagering.NewPendingOptions {
	return wagering.NewPendingOptions{
		ID:             externalID + "-int",
		ExternalTxID:   externalID,
		ProviderID:     "provider-a",
		PlayerID:       "player-1",
		WalletID:       "wallet-1",
		RoundID:        "round-1",
		GameID:         "g1",
		Kind:           kind,
		Money:          amt,
		ReferenceExtID: refExt,
		IdempotencyKey: "provider-a:" + externalID,
		OccurredAt:     time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC),
	}
}

func newPendingWithOptions(t *testing.T, opts wagering.NewPendingOptions) wagering.Transaction {
	t.Helper()
	tx, err := wagering.NewPending(opts)
	if err != nil {
		t.Fatal(err)
	}
	return tx
}

func processedTx(t *testing.T, kind wagering.Kind, externalID, refExt string, amt money.Money) wagering.Transaction {
	t.Helper()
	tx, err := wagering.NewPending(basePendingOptions(kind, externalID, refExt, amt))
	if err != nil {
		t.Fatal(err)
	}
	tx, err = tx.Process(2, mustMoneyT(t, "900.00"), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	return tx
}

// TestValidateReversalReferenceKind garante a compatibilidade de tipo exigida
// em §7: REFUND só devolve uma BET processada; ROLLBACK desfaz BET, WIN ou
// REFUND. Um REFUND contra WIN jamais movimenta a carteira.
func TestValidateReversalReferenceKind(t *testing.T) {
	amt := mustMoneyT(t, "25.00")
	bet := processedTx(t, wagering.KindBet, "bet-1", "", amt)
	win := processedTx(t, wagering.KindWin, "win-1", "", amt)
	refund := processedTx(t, wagering.KindRefund, "ref-1", "bet-1", amt)

	refundOverBet := pendingTx(t, wagering.KindRefund, "r-1", "bet-1", amt)
	if err := validateReversal(refundOverBet, bet); err != nil {
		t.Errorf("REFUND de BET = %v, want nil", err)
	}

	rollbackOverBet := pendingTx(t, wagering.KindRollback, "r-2", "bet-1", amt)
	if err := validateReversal(rollbackOverBet, bet); err != nil {
		t.Errorf("ROLLBACK de BET = %v, want nil", err)
	}
	rollbackOverWin := pendingTx(t, wagering.KindRollback, "r-3", "win-1", amt)
	if err := validateReversal(rollbackOverWin, win); err != nil {
		t.Errorf("ROLLBACK de WIN = %v, want nil", err)
	}
	rollbackOverRefund := pendingTx(t, wagering.KindRollback, "r-4", "ref-1", amt)
	if err := validateReversal(rollbackOverRefund, refund); err != nil {
		t.Errorf("ROLLBACK de REFUND = %v, want nil", err)
	}

	// Combinações incompatíveis.
	refundOverWin := pendingTx(t, wagering.KindRefund, "r-5", "win-1", amt)
	if err := validateReversal(refundOverWin, win); !errors.Is(err, derr.ErrReversalMismatch) {
		t.Errorf("REFUND de WIN = %v, want ErrReversalMismatch", err)
	}
	refundOverRefund := pendingTx(t, wagering.KindRefund, "r-6", "ref-1", amt)
	if err := validateReversal(refundOverRefund, refund); !errors.Is(err, derr.ErrReversalMismatch) {
		t.Errorf("REFUND de REFUND = %v, want ErrReversalMismatch", err)
	}
}

// TestValidateReversalAgreement exige igualdade de provedor, jogador, carteira,
// moeda e rodada, e valor idêntico ao da referência.
func TestValidateReversalAgreement(t *testing.T) {
	amt := mustMoneyT(t, "25.00")
	bet := processedTx(t, wagering.KindBet, "bet-1", "", amt)

	cases := []struct {
		name string
		opts func(wagering.NewPendingOptions) wagering.NewPendingOptions
	}{
		{"provedor", func(p wagering.NewPendingOptions) wagering.NewPendingOptions { p.ProviderID = "provider-b"; return p }},
		{"jogador", func(p wagering.NewPendingOptions) wagering.NewPendingOptions { p.PlayerID = "player-2"; return p }},
		{"carteira", func(p wagering.NewPendingOptions) wagering.NewPendingOptions { p.WalletID = "wallet-2"; return p }},
		{"rodada", func(p wagering.NewPendingOptions) wagering.NewPendingOptions { p.RoundID = "round-2"; return p }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tx := newPendingWithOptions(t, tc.opts(basePendingOptions(wagering.KindRefund, "r-x", "bet-1", amt)))
			if err := validateReversal(tx, bet); !errors.Is(err, derr.ErrReversalMismatch) {
				t.Errorf("err = %v, want ErrReversalMismatch (%s)", err, tc.name)
			}
		})
	}

	// Valor diferente do referenciado também é mismatch.
	other := pendingTx(t, wagering.KindRefund, "r-y", "bet-1", mustMoneyT(t, "30.00"))
	if err := validateReversal(other, bet); !errors.Is(err, derr.ErrReversalMismatch) {
		t.Errorf("valor diferente: err = %v, want ErrReversalMismatch", err)
	}
}

// TestReversalEffect garante que REFUND e ROLLBACK de uma BET compartilham o
// mesmo efeito financeiro (CREDIT — devolução do débito), enquanto ROLLBACK de
// WIN/REFUND tem efeito DEBIT. É a base para impedir a devolução duplicada do
// mesmo débito.
func TestReversalEffect(t *testing.T) {
	amt := mustMoneyT(t, "25.00")
	bet := processedTx(t, wagering.KindBet, "bet-1", "", amt)
	win := processedTx(t, wagering.KindWin, "win-1", "", amt)
	refunded := processedTx(t, wagering.KindRefund, "ref-1", "bet-1", amt)

	if got := reversalEffect(pendingTx(t, wagering.KindRefund, "r-1", "bet-1", amt), bet); got != "CREDIT" {
		t.Errorf("REFUND(BET) effect = %q, want CREDIT", got)
	}
	if got := reversalEffect(pendingTx(t, wagering.KindRollback, "r-2", "bet-1", amt), bet); got != "CREDIT" {
		t.Errorf("ROLLBACK(BET) effect = %q, want CREDIT", got)
	}
	if got := reversalEffect(pendingTx(t, wagering.KindRollback, "r-3", "win-1", amt), win); got != "DEBIT" {
		t.Errorf("ROLLBACK(WIN) effect = %q, want DEBIT", got)
	}
	if got := reversalEffect(pendingTx(t, wagering.KindRollback, "r-4", "ref-1", amt), refunded); got != "DEBIT" {
		t.Errorf("ROLLBACK(REFUND) effect = %q, want DEBIT", got)
	}
}

// TestReversalIsDebit valida a direção efetiva do movimento na carteira.
func TestReversalIsDebit(t *testing.T) {
	amt := mustMoneyT(t, "25.00")
	bet := processedTx(t, wagering.KindBet, "bet-1", "", amt)
	win := processedTx(t, wagering.KindWin, "win-1", "", amt)

	if reversalIsDebit(pendingTx(t, wagering.KindRollback, "r-1", "bet-1", amt), bet) {
		t.Error("ROLLBACK(BET) deveria CREDITAR (devolve o débito)")
	}
	if !reversalIsDebit(pendingTx(t, wagering.KindRollback, "r-2", "win-1", amt), win) {
		t.Error("ROLLBACK(WIN) deveria DEBITAR (desfaz o crédito)")
	}
}
