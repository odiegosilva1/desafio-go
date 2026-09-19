package messaging

import (
	"encoding/json"
	"fmt"
	"time"
)

// MoneyJSON é o formato monetário no corpo da mensagem.
type MoneyJSON struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

// InboundEnvelope é o corpo das mensagens recebidas da fila de operações.
// Tipo exportado para manter o contrato serializado do wire do consumidor.
type InboundEnvelope struct {
	MessageID  string           `json:"messageId"`
	Type       string           `json:"type"`
	OccurredAt time.Time        `json:"occurredAt"`
	Data       InboundWagerData `json:"data"`
}

// InboundWagerData são os dados de negócio do envelope de operação.
type InboundWagerData struct {
	ProviderID                     string    `json:"providerId"`
	ExternalTransactionID          string    `json:"externalTransactionId"`
	IdempotencyKey                 string    `json:"idempotencyKey"`
	PlayerID                       string    `json:"playerId"`
	WalletID                       string    `json:"walletId"`
	RoundID                        string    `json:"roundId"`
	GameID                         string    `json:"gameId"`
	Kind                           string    `json:"kind"`
	Money                          MoneyJSON `json:"money"`
	ReferenceExternalTransactionID string    `json:"referenceExternalTransactionId,omitempty"`
	CorrelationID                  string    `json:"correlationId,omitempty"`
}

// ParseEnvelope valida e decodifica o corpo de uma mensagem da fila de
// operações. Rejeita envelopes sem messageId ou externalTransactionId, que
// seriam encaminhados à DLQ como INVALID_MESSAGE.
func ParseEnvelope(body string) (InboundEnvelope, error) {
	var env InboundEnvelope
	if err := json.Unmarshal([]byte(body), &env); err != nil {
		return env, fmt.Errorf("messaging: malformed envelope: %w", err)
	}
	if env.MessageID == "" || env.Data.ExternalTransactionID == "" {
		return env, fmt.Errorf("messaging: missing messageId/externalTransactionId")
	}
	return env, nil
}

// MarshalInbound serializa o envelope no formato canônico da fila de operações.
func MarshalInbound(env InboundEnvelope) ([]byte, error) {
	return json.Marshal(env)
}
