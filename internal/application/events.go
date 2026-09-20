package application

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"

	"desafio-go/internal/domain/event"
	"desafio-go/internal/domain/money"
	"desafio-go/internal/domain/wagering"
	"desafio-go/internal/domain/wallet"
	"desafio-go/internal/storage/port"
)

// toMoneyJSON converte Money no formato de contrato {"amount","currency"}.
func toMoneyJSON(m money.Money) event.MoneyJSON {
	return event.MoneyJSON{Amount: m.Amount(), Currency: string(m.Currency())}
}

// newUUID gera um identificador opaco estável para eventos e transações.
func newUUID() string {
	return uuid.NewString()
}

// record converte um Envelope em um registro de outbox com payload JSON.
func record(env event.Envelope) (port.OutboxRecord, error) {
	payload, err := json.Marshal(env)
	if err != nil {
		return port.OutboxRecord{}, err
	}
	return port.OutboxRecord{
		EventID:       env.EventID,
		AggregateType: aggregateType(env.EventType),
		AggregateID:   env.AggregateID,
		EventType:     string(env.EventType),
		CorrelationID: env.CorrelationID,
		CausationID:   env.CausationID,
		OccurredAt:    env.OccurredAt,
		Version:       env.Version,
		Payload:       payload,
	}, nil
}

func aggregateType(t event.EventType) string {
	switch t {
	case event.TypeWalletBalanceChanged:
		return "Wallet"
	default:
		return "WagerTransaction"
	}
}

// record converte um Envelope em um registro de outbox com payload JSON.

// processedEvent gera WagerTransactionProcessed para uma operação concluída.
func processedEvent(t wagering.Transaction, now time.Time) (port.OutboxRecord, error) {
	data := event.WagerTransactionProcessedData{
		TransactionID:                  t.ID(),
		ExternalTransactionID:          t.ExternalID(),
		ProviderID:                     t.ProviderID(),
		WalletID:                       t.WalletID(),
		PlayerID:                       t.PlayerID(),
		RoundID:                        t.RoundID(),
		GameID:                         t.GameID(),
		Kind:                           string(t.Kind()),
		Money:                          toMoneyJSON(t.Money()),
		ReferenceExternalTransactionID: t.ReferenceExtID(),
		WalletVersion:                  t.WalletVersion(),
	}
	env := event.NewWagerTransactionProcessed(
		newUUID(), t.ID(), t.CorrelationID(), now, data)
	return record(env)
}

// rejectedEvent gera WagerTransactionRejected para uma rejeição definitiva.
func rejectedEvent(t wagering.Transaction, now time.Time) (port.OutboxRecord, error) {
	data := event.WagerTransactionRejectedData{
		TransactionID:         t.ID(),
		ExternalTransactionID: t.ExternalID(),
		ProviderID:            t.ProviderID(),
		WalletID:              t.WalletID(),
		PlayerID:              t.PlayerID(),
		RoundID:               t.RoundID(),
		GameID:                t.GameID(),
		Kind:                  string(t.Kind()),
		Money:                 toMoneyJSON(t.Money()),
		FailureCode:           t.FailureCode(),
		FailureMessage:        t.FailureMessage(),
	}
	env := event.NewWagerTransactionRejected(
		newUUID(), t.ID(), t.CorrelationID(), now, data)
	return record(env)
}

// pendingReferenceEvent gera WagerTransactionPendingReference para a espera por
// referência.
func pendingReferenceEvent(t wagering.Transaction, now time.Time) (port.OutboxRecord, error) {
	data := event.WagerTransactionPendingReferenceData{
		TransactionID:                  t.ID(),
		ExternalTransactionID:          t.ExternalID(),
		ProviderID:                     t.ProviderID(),
		WalletID:                       t.WalletID(),
		ReferenceExternalTransactionID: t.ReferenceExtID(),
		Kind:                           string(t.Kind()),
	}
	env := event.NewWagerTransactionPendingReference(
		newUUID(), t.ID(), t.CorrelationID(), now, data)
	return record(env)
}

// balanceChangedEvent gera WalletBalanceChanged a partir da movimentação.
func balanceChangedEvent(m wallet.Movement, correlationID string, now time.Time) (port.OutboxRecord, error) {
	data := event.WalletBalanceChangedData{
		WalletID:      m.WalletID(),
		TransactionID: m.TransactionID(),
		Direction:     event.Direction(m.Direction()),
		Money:         toMoneyJSON(m.Amount()),
		BalanceBefore: toMoneyJSON(m.BalanceBefore()),
		BalanceAfter:  toMoneyJSON(m.BalanceAfter()),
		WalletVersion: m.WalletVersion(),
	}
	env := event.NewWalletBalanceChanged(
		newUUID(), m.WalletID(), correlationID, now, data)
	return record(env)
}
