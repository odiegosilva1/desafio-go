package wagering

import (
	"errors"
	"testing"
	"time"

	"desafio-go/internal/domain/money"
)

func mustMoney(t *testing.T, amount string) money.Money {
	t.Helper()
	m, err := money.FromDecimalString(amount, money.CurrencyBRL)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestNewPendingBet(t *testing.T) {
	tx, err := NewPending(NewPendingOptions{
		ID:             "tx-1",
		ExternalTxID:   "external-1",
		ProviderID:     "provider-a",
		PlayerID:       "player-1",
		WalletID:       "wallet-1",
		RoundID:        "round-1",
		GameID:         "fortune-chimp",
		Kind:           KindBet,
		Money:          mustMoney(t, "25.00"),
		IdempotencyKey: "provider-a:external-1",
		OccurredAt:     time.Now(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if tx.State() != StatePending {
		t.Errorf("State = %q, want PENDING", tx.State())
	}
	if tx.Effect().Direction != DirectionDebit {
		t.Errorf("Effect.Direction = %q, want DEBIT", tx.Effect().Direction)
	}
	if tx.Effect().ReferenceRequired {
		t.Error("BET não exige referência")
	}
	if tx.PayloadHash() == "" {
		t.Error("PayloadHash vazio")
	}
}

func TestNewPendingLossRequiresZeroAmount(t *testing.T) {
	if _, err := NewPending(NewPendingOptions{
		ID: "tx-1", ExternalTxID: "e-1", ProviderID: "p", PlayerID: "pl",
		WalletID: "w", RoundID: "r", GameID: "g", Kind: KindLoss,
		Money: mustMoney(t, "5.00"),
	}); !errors.Is(err, ErrLossRequiresZero) {
		t.Errorf("err = %v, want ErrLossRequiresZero", err)
	}

	tx, err := NewPending(NewPendingOptions{
		ID: "tx-1", ExternalTxID: "e-1", ProviderID: "p", PlayerID: "pl",
		WalletID: "w", RoundID: "r", GameID: "g", Kind: KindLoss,
		Money: mustMoney(t, "0.00"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if tx.Effect().Direction != DirectionNone {
		t.Errorf("LOSS Effect.Direction = %q, want NONE", tx.Effect().Direction)
	}
}

func TestNewPendingRefundRequiresReference(t *testing.T) {
	if _, err := NewPending(NewPendingOptions{
		ID: "tx-1", ExternalTxID: "e-1", ProviderID: "p", PlayerID: "pl",
		WalletID: "w", RoundID: "r", GameID: "g", Kind: KindRefund,
		Money: mustMoney(t, "25.00"),
	}); !errors.Is(err, ErrRefundRequiresRef) {
		t.Errorf("err = %v, want ErrRefundRequiresRef", err)
	}
}

func TestOpeningBlockedOverExternal(t *testing.T) {
	if _, err := NewPending(NewPendingOptions{
		ID: "tx-1", ExternalTxID: "e-1", ProviderID: "p", PlayerID: "pl",
		WalletID: "w", RoundID: "r", GameID: "g", Kind: KindOpening,
		Money: mustMoney(t, "0.00"),
	}); !errors.Is(err, ErrOpeningInternal) {
		t.Errorf("err = %v, want ErrOpeningInternal", err)
	}
}

func TestStateMachine(t *testing.T) {
	tx, err := NewPending(NewPendingOptions{
		ID: "tx-1", ExternalTxID: "e-1", ProviderID: "p", PlayerID: "pl",
		WalletID: "w", RoundID: "r", GameID: "g", Kind: KindBet,
		Money: mustMoney(t, "25.00"),
	})
	if err != nil {
		t.Fatal(err)
	}

	pendRef, err := tx.MarkPendingReference(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if pendRef.State() != StatePendingReference {
		t.Errorf("State = %q, want PENDING_REFERENCE", pendRef.State())
	}

	back, err := pendRef.ResolveReference(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if back.State() != StatePending {
		t.Errorf("State = %q, want PENDING após resolução", back.State())
	}

	processed, err := back.Process(3, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if processed.State() != StateProcessed {
		t.Errorf("State = %q, want PROCESSED", processed.State())
	}
	if processed.WalletVersion() != 3 {
		t.Errorf("WalletVersion = %d, want 3", processed.WalletVersion())
	}
	if processed.IsTerminal() != true {
		t.Error("PROCESSED deveria ser terminal")
	}
	if _, err := processed.MarkPendingReference(time.Now()); !errors.Is(err, ErrTerminalState) {
		t.Errorf("transição terminal err = %v, want ErrTerminalState", err)
	}
}

func TestResolveRequiresPendingReference(t *testing.T) {
	tx, _ := NewPending(NewPendingOptions{
		ID: "tx-1", ExternalTxID: "e-1", ProviderID: "p", PlayerID: "pl",
		WalletID: "w", RoundID: "r", GameID: "g", Kind: KindBet,
		Money: mustMoney(t, "25.00"),
	})
	if _, err := tx.ResolveReference(time.Now()); !errors.Is(err, ErrNotPendingReference) {
		t.Errorf("err = %v, want ErrNotPendingReference", err)
	}
}

func TestRejectAndFailAreTerminal(t *testing.T) {
	tx, _ := NewPending(NewPendingOptions{
		ID: "tx-1", ExternalTxID: "e-1", ProviderID: "p", PlayerID: "pl",
		WalletID: "w", RoundID: "r", GameID: "g", Kind: KindBet,
		Money: mustMoney(t, "25.00"),
	})

	rej, err := tx.Reject("INSUFFICIENT_FUNDS", "saldo", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if rej.State() != StateRejected || rej.FailureCode() != "INSUFFICIENT_FUNDS" {
		t.Errorf("Reject State/FailureCode = %q/%q", rej.State(), rej.FailureCode())
	}

	fail, err := tx.Fail("TRANSIENT", "db", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if fail.State() != StateFailed {
		t.Errorf("Fail State = %q, want FAILED", fail.State())
	}
}

func TestRehydrateRoundtrip(t *testing.T) {
	original, err := NewPending(NewPendingOptions{
		ID: "tx-1", ExternalTxID: "e-1", ProviderID: "p", PlayerID: "pl",
		WalletID: "w", RoundID: "r", GameID: "g", Kind: KindBet,
		Money: mustMoney(t, "25.00"),
	})
	if err != nil {
		t.Fatal(err)
	}

	rehydrated, err := Rehydrate(RehydrateOptions{
		ID: original.ID(), ExternalTxID: original.ExternalID(),
		ProviderID: original.ProviderID(), PlayerID: original.PlayerID(),
		WalletID: original.WalletID(), RoundID: original.RoundID(),
		GameID: original.GameID(), Kind: original.Kind(),
		Money: original.Money(), ReferenceExtID: original.ReferenceExtID(),
		IdempotencyKey: original.IdempotencyKey(),
		CorrelationID:  original.CorrelationID(), CausationID: original.CausationID(),
		OccurredAt: original.OccurredAt(), RecordedAt: original.RecordedAt(),
		PayloadHash: original.PayloadHash(), State: original.State(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if rehydrated.PayloadHash() != original.PayloadHash() {
		t.Errorf("PayloadHash = %q, want %q (reidratação não recalcula)", rehydrated.PayloadHash(), original.PayloadHash())
	}
	if rehydrated.CausationID() != original.CausationID() {
		t.Errorf("CausationID = %q, want %q", rehydrated.CausationID(), original.CausationID())
	}
}

func TestHTTPAndSQSShareHash(t *testing.T) {
	httpTx, err := NewPending(NewPendingOptions{
		ID: "tx-1", ExternalTxID: "e-1", ProviderID: "p", PlayerID: "pl",
		WalletID: "w", RoundID: "r", GameID: "g", Kind: KindBet,
		Money:          mustMoney(t, "25.00"),
		IdempotencyKey: "k-http",
	})
	if err != nil {
		t.Fatal(err)
	}
	sqsTx, err := NewPending(NewPendingOptions{
		ID: "tx-1", ExternalTxID: "e-1", ProviderID: "p", PlayerID: "pl",
		WalletID: "w", RoundID: "r", GameID: "g", Kind: KindBet,
		Money:          mustMoney(t, "25.00"),
		IdempotencyKey: "k-sqs",
	})
	if err != nil {
		t.Fatal(err)
	}
	if httpTx.PayloadHash() != sqsTx.PayloadHash() {
		t.Errorf("hash HTTP (%q) != hash SQS (%q) — devem coincidir", httpTx.PayloadHash(), sqsTx.PayloadHash())
	}
}
