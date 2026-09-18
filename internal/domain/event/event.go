// Package event define o envelope e os tipos de eventos de integração do
// domínio. Cada construtor define o eventType e a versão do snapshot de dados.
// Os payloads são snapshots imutáveis usados pela transactional outbox.
package event

import "time"

// EventType é o identificador do tipo de evento (ex.: "WagerTransactionProcessed").
type EventType string

// Tipos de eventos exigidos pelo desafio.
const (
	TypeWagerTransactionProcessed        EventType = "WagerTransactionProcessed"
	TypeWagerTransactionRejected         EventType = "WagerTransactionRejected"
	TypeWagerTransactionPendingReference EventType = "WagerTransactionPendingReference"
	TypeWalletBalanceChanged             EventType = "WalletBalanceChanged"
)

// Direction indica a direção de um lançamento de ledger.
type Direction string

const (
	DirectionDebit  Direction = "DEBIT"
	DirectionCredit Direction = "CREDIT"
)

// Versões dos snapshots de dados de cada evento.
const (
	VersionWagerTransactionProcessed        = 1
	VersionWagerTransactionRejected         = 1
	VersionWagerTransactionPendingReference = 1
	VersionWalletBalanceChanged             = 1
)

// Envelope é o invólucro de transporte/persistência de um evento.
// Persistido pela outbox; eventId é estável em republicações.
type Envelope struct {
	EventID       string    `json:"eventId"`
	EventType     EventType `json:"eventType"`
	AggregateID   string    `json:"aggregateId"`
	CorrelationID string    `json:"correlationId"`
	CausationID   string    `json:"causationId,omitempty"`
	OccurredAt    time.Time `json:"occurredAt"`
	Version       int       `json:"version"`
	Data          any       `json:"data"`
}

// WagerTransactionProcessedData é o snapshot de uma operação concluída com
// sucesso, incluindo LOSS (neste caso walletVersion é omitido ou 0).
type WagerTransactionProcessedData struct {
	TransactionID                  string    `json:"transactionId"`
	ExternalTransactionID          string    `json:"externalTransactionId"`
	ProviderID                     string    `json:"providerId"`
	WalletID                       string    `json:"walletId"`
	PlayerID                       string    `json:"playerId"`
	RoundID                        string    `json:"roundId"`
	GameID                         string    `json:"gameId"`
	Kind                           string    `json:"kind"`
	Money                          moneyJSON `json:"money"`
	ReferenceExternalTransactionID string    `json:"referenceExternalTransactionId,omitempty"`
	WalletVersion                  int       `json:"walletVersion,omitempty"`
}

// WagerTransactionRejectedData é o snapshot de uma rejeição definitiva por
// regra de negócio, com failureCode estável.
type WagerTransactionRejectedData struct {
	TransactionID         string    `json:"transactionId"`
	ExternalTransactionID string    `json:"externalTransactionId"`
	ProviderID            string    `json:"providerId"`
	WalletID              string    `json:"walletId"`
	PlayerID              string    `json:"playerId"`
	RoundID               string    `json:"roundId"`
	GameID                string    `json:"gameId"`
	Kind                  string    `json:"kind"`
	Money                 moneyJSON `json:"money"`
	FailureCode           string    `json:"failureCode"`
	FailureMessage        string    `json:"failureMessage,omitempty"`
}

// WagerTransactionPendingReferenceData é o snapshot do registro de espera por
// referência.
type WagerTransactionPendingReferenceData struct {
	TransactionID                  string `json:"transactionId"`
	ExternalTransactionID          string `json:"externalTransactionId"`
	ProviderID                     string `json:"providerId"`
	WalletID                       string `json:"walletId"`
	ReferenceExternalTransactionID string `json:"referenceExternalTransactionId"`
	Kind                           string `json:"kind"`
}

// WalletBalanceChangedData é o snapshot de uma alteração efetiva de saldo.
type WalletBalanceChangedData struct {
	WalletID      string    `json:"walletId"`
	TransactionID string    `json:"transactionId"`
	Direction     Direction `json:"direction"`
	Money         moneyJSON `json:"money"`
	BalanceBefore moneyJSON `json:"balanceBefore"`
	BalanceAfter  moneyJSON `json:"balanceAfter"`
	WalletVersion int       `json:"walletVersion"`
}

// moneyJSON evita importar o pacote money e cria dependência circular com
// wagering. Mapeado para {"amount":"25.00","currency":"BRL"}.
type moneyJSON struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

// NewWagerTransactionProcessed cria um evento de operação concluída.
func NewWagerTransactionProcessed(
	eventID, aggregateID, correlationID string,
	occurredAt time.Time,
	d WagerTransactionProcessedData,
) Envelope {
	return newEnvelope(eventID, TypeWagerTransactionProcessed, aggregateID, correlationID,
		VersionWagerTransactionProcessed, occurredAt, d)
}

// NewWagerTransactionRejected cria um evento de rejeição definitiva.
func NewWagerTransactionRejected(
	eventID, aggregateID, correlationID string,
	occurredAt time.Time,
	d WagerTransactionRejectedData,
) Envelope {
	return newEnvelope(eventID, TypeWagerTransactionRejected, aggregateID, correlationID,
		VersionWagerTransactionRejected, occurredAt, d)
}

// NewWagerTransactionPendingReference cria um evento de pendência por referência.
func NewWagerTransactionPendingReference(
	eventID, aggregateID, correlationID string,
	occurredAt time.Time,
	d WagerTransactionPendingReferenceData,
) Envelope {
	return newEnvelope(eventID, TypeWagerTransactionPendingReference, aggregateID, correlationID,
		VersionWagerTransactionPendingReference, occurredAt, d)
}

// NewWalletBalanceChanged cria um evento de alteração efetiva de saldo.
func NewWalletBalanceChanged(
	eventID, aggregateID, correlationID string,
	occurredAt time.Time,
	d WalletBalanceChangedData,
) Envelope {
	return newEnvelope(eventID, TypeWalletBalanceChanged, aggregateID, correlationID,
		VersionWalletBalanceChanged, occurredAt, d)
}

func newEnvelope(
	eventID string,
	eventType EventType,
	aggregateID, correlationID string,
	version int,
	occurredAt time.Time,
	data any,
) Envelope {
	return Envelope{
		EventID:       eventID,
		EventType:     eventType,
		AggregateID:   aggregateID,
		CorrelationID: correlationID,
		OccurredAt:    occurredAt.UTC(),
		Version:       version,
		Data:          data,
	}
}

// WithCausation define o causationId do evento (encadeamento com eventos que
// originaram o processamento). Utiliza-se para rastrear causalidade.
func (e Envelope) WithCausation(causationID string) Envelope {
	e.CausationID = causationID
	return e
}

// TypeOf expose o tipo do evento.
func (e Envelope) TypeOf() EventType { return e.EventType }
