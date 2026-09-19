package event

import (
	"encoding/json"
	"testing"
	"time"
)

func mustTime(t *testing.T, s string) time.Time {
	t.Helper()
	ts, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t.Fatal(err)
	}
	return ts
}

func TestWalletBalanceChangedConstructor(t *testing.T) {
	at := mustTime(t, "2026-09-08T12:00:00Z")
	ev := NewWalletBalanceChanged(
		"evt-1", "wallet-1", "corr-1", at,
		WalletBalanceChangedData{
			WalletID:      "wallet-1",
			TransactionID: "tx-1",
			Direction:     DirectionDebit,
			Money:         MoneyJSON{Amount: "25.00", Currency: "BRL"},
			BalanceBefore: MoneyJSON{Amount: "100.00", Currency: "BRL"},
			BalanceAfter:  MoneyJSON{Amount: "75.00", Currency: "BRL"},
			WalletVersion: 2,
		},
	)

	if ev.EventType != TypeWalletBalanceChanged {
		t.Errorf("EventType = %q", ev.EventType)
	}
	if ev.Version != VersionWalletBalanceChanged {
		t.Errorf("Version = %d", ev.Version)
	}
	if ev.AggregateID != "wallet-1" {
		t.Errorf("AggregateID = %q", ev.AggregateID)
	}

	b, err := json.Marshal(ev)
	if err != nil {
		t.Fatal(err)
	}
	var round struct {
		EventType EventType   `json:"eventType"`
		Data      interface{} `json:"data"`
	}
	if err := json.Unmarshal(b, &round); err != nil {
		t.Fatal(err)
	}
	raw := string(b)
	for _, want := range []string{
		`"eventType":"WalletBalanceChanged"`,
		`"direction":"DEBIT"`,
		`"money":{"amount":"25.00","currency":"BRL"}`,
		`"balanceBefore":{"amount":"100.00","currency":"BRL"}`,
		`"walletVersion":2`,
	} {
		if !contains(raw, want) {
			t.Errorf("marshal não contém %s: %s", want, raw)
		}
	}
}

func TestProcessedAndRejectedConstructors(t *testing.T) {
	at := mustTime(t, "2026-09-08T12:00:00Z")

	processed := NewWagerTransactionProcessed("evt-2", "tx-1", "corr-2", at,
		WagerTransactionProcessedData{
			TransactionID:                  "tx-1",
			ExternalTransactionID:          "transaction-123",
			ProviderID:                     "provider-a",
			WalletID:                       "wallet-1",
			PlayerID:                       "player-1",
			RoundID:                        "round-987",
			GameID:                         "fortune-chimp",
			Kind:                           "BET",
			Money:                          MoneyJSON{Amount: "25.00", Currency: "BRL"},
			ReferenceExternalTransactionID: "",
			WalletVersion:                  2,
		})
	if processed.EventType != TypeWagerTransactionProcessed {
		t.Errorf("EventType = %q", processed.EventType)
	}

	rejected := NewWagerTransactionRejected("evt-3", "tx-2", "corr-3", at,
		WagerTransactionRejectedData{
			TransactionID:         "tx-2",
			ExternalTransactionID: "transaction-456",
			ProviderID:            "provider-a",
			Kind:                  "BET",
			FailureCode:           "INSUFFICIENT_FUNDS",
			FailureMessage:        "saldo insuficiente",
		})
	if rejected.EventType != TypeWagerTransactionRejected {
		t.Errorf("EventType = %q", rejected.EventType)
	}
	if rejected.CausationID != "" {
		t.Error("causation deveria estar vazio por padrão")
	}
}

func TestPendingReferenceAndCausation(t *testing.T) {
	at := mustTime(t, "2026-09-08T12:00:00Z")
	ev := NewWagerTransactionPendingReference("evt-4", "tx-3", "corr-4", at,
		WagerTransactionPendingReferenceData{
			TransactionID:                  "tx-3",
			ExternalTransactionID:          "transaction-789",
			ProviderID:                     "provider-a",
			ReferenceExternalTransactionID: "transaction-123",
			Kind:                           "REFUND",
		}).WithCausation("evt-2")

	if ev.EventType != TypeWagerTransactionPendingReference {
		t.Errorf("EventType = %q", ev.EventType)
	}
	if ev.CausationID != "evt-2" {
		t.Errorf("CausationID = %q", ev.CausationID)
	}
}

func TestOccurredAtUTC(t *testing.T) {
	local := time.Date(2026, 9, 8, 9, 0, 0, 0, time.FixedZone("BRT", -3*3600))
	ev := NewWalletBalanceChanged("evt", "w", "c", local, WalletBalanceChangedData{})
	if ev.OccurredAt.Location() != time.UTC {
		t.Error("OccurredAt deveria estar em UTC")
	}
	if !ev.OccurredAt.Equal(time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)) {
		t.Errorf("OccurredAt = %v, want 12:00:00Z", ev.OccurredAt)
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (func() bool {
		for i := 0; i+len(sub) <= len(s); i++ {
			if s[i:i+len(sub)] == sub {
				return true
			}
		}
		return false
	})()
}
