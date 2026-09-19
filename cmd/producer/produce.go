// Command producer enfileira transações de aposta em wager-transactions.fifo
// com o mesmo wire aceito pelo consumidor SQS (internal/messaging). Serve para
// cenários E2E, stress e demonstração sem passar pelo HTTP.
//
// As convenções FIFO são explícitas: MessageGroupId = data.walletId (ordem por
// carteira) e MessageDeduplicationId = messageId do envelope (estável). Consulte
// ARCHITECTURE.md e o resultado de `producer sample`.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/google/uuid"

	"desafio-go/internal/config"
	"desafio-go/internal/domain/money"
	"desafio-go/internal/domain/wagering"
	"desafio-go/internal/messaging"
	"desafio-go/internal/observability"
)

// sendOptions controla o chaveamento FIFO das mensagens produzidas.
type sendOptions struct {
	// groupOverride força MessageGroupId; vazio usa data.walletId.
	groupOverride string
	// dedupOverride força MessageDeduplicationId; vazio usa o messageId.
	dedupOverride string
	// repeat enfileira cópias idênticas regenerando o messageId por cópia.
	repeat int
}

// readEnvelopes decodifica um documento JSON com um envelope ou uma lista.
func readEnvelopes(r io.Reader) ([]messaging.InboundEnvelope, error) {
	var raw json.RawMessage
	dec := json.NewDecoder(r)
	if err := dec.Decode(&raw); err != nil {
		return nil, fmt.Errorf("producer: decodificar JSON: %w", err)
	}
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return nil, fmt.Errorf("producer: documento vazio")
	}

	if raw[0] == '[' {
		var list []messaging.InboundEnvelope
		if err := json.Unmarshal(raw, &list); err != nil {
			return nil, fmt.Errorf("producer: decodificar lista de envelopes: %w", err)
		}
		if len(list) == 0 {
			return nil, fmt.Errorf("producer: lista vazia")
		}
		return list, nil
	}

	var single messaging.InboundEnvelope
	if err := json.Unmarshal(raw, &single); err != nil {
		return nil, fmt.Errorf("producer: decodificar envelope: %w", err)
	}
	return []messaging.InboundEnvelope{single}, nil
}

// validateInbound aplica as mesmas regras estruturais do consumidor antes do
// envio, evitando mensagens que só seriam descobertas na DLQ.
func validateInbound(env messaging.InboundEnvelope) error {
	d := env.Data
	var missing []string
	if env.MessageID == "" {
		missing = append(missing, "messageId")
	}
	for name, v := range map[string]string{
		"providerId":            d.ProviderID,
		"externalTransactionId": d.ExternalTransactionID,
		"idempotencyKey":        d.IdempotencyKey,
		"playerId":              d.PlayerID,
		"walletId":              d.WalletID,
		"roundId":               d.RoundID,
		"gameId":                d.GameID,
		"kind":                  d.Kind,
		"amount":                d.Money.Amount,
		"currency":              d.Money.Currency,
	} {
		if v == "" {
			missing = append(missing, "data."+name)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("producer: campos obrigatórios vazios: %s", strings.Join(missing, ", "))
	}

	m, err := money.FromDecimalString(d.Money.Amount, money.Currency(d.Money.Currency))
	if err != nil {
		return fmt.Errorf("producer: money inválido: %w", err)
	}
	switch wagering.Kind(d.Kind) {
	case wagering.KindBet, wagering.KindWin, wagering.KindRefund, wagering.KindRollback:
		if m.IsZero() || m.IsNegative() {
			return fmt.Errorf("producer: %s exige amount > 0", d.Kind)
		}
	case wagering.KindLoss:
		if !m.IsZero() {
			return fmt.Errorf("producer: LOSS exige amount 0.00")
		}
	default:
		return fmt.Errorf("producer: kind %q não é enviável (BET|WIN|LOSS|REFUND|ROLLBACK)", d.Kind)
	}
	return nil
}

// sendEnvelopes enfileira os envelopes aplicando o chaveamento FIFO. Retorna a
// quantidade enviada. Com repeat > 1 exige messageId vazio (regenerado por
// cópia para não colidir na janela de deduplicação).
func sendEnvelopes(ctx context.Context, client messaging.SQSClient, queueURL string, envs []messaging.InboundEnvelope, opts sendOptions, logger *slog.Logger) (int, error) {
	if opts.repeat < 1 {
		opts.repeat = 1
	}
	if opts.repeat > 1 {
		for _, env := range envs {
			if env.MessageID != "" {
				return 0, fmt.Errorf("producer: -repeat > 1 exige messageId vazio (messageId é regenerado por cópia)")
			}
		}
	}

	sent := 0
	for i := 0; i < opts.repeat; i++ {
		for _, env := range envs {
			e := env
			if opts.repeat > 1 || e.MessageID == "" {
				e.MessageID = uuid.NewString()
			}
			if e.OccurredAt.IsZero() {
				e.OccurredAt = time.Now().UTC()
			}
			if err := validateInbound(e); err != nil {
				return sent, err
			}

			group := opts.groupOverride
			if group == "" {
				group = e.Data.WalletID
			}
			dedup := opts.dedupOverride
			if dedup == "" {
				dedup = e.MessageID
			}

			body, err := messaging.MarshalInbound(e)
			if err != nil {
				return sent, err
			}
			if _, err := client.SendMessage(ctx, &sqs.SendMessageInput{
				QueueUrl:               aws.String(queueURL),
				MessageBody:            aws.String(string(body)),
				MessageGroupId:         aws.String(group),
				MessageDeduplicationId: aws.String(dedup),
			}); err != nil {
				return sent, fmt.Errorf("producer: enviar para %s: %w", queueURL, err)
			}
			observability.Info(ctx, logger, "wager transaction enqueued",
				"walletId", e.Data.WalletID, "transactionId", e.Data.ExternalTransactionID,
				"kind", e.Data.Kind, "messageId", e.MessageID)
			sent++
		}
	}
	return sent, nil
}

// wagerOptions descreve o envelope construído pelo subcomando wager.
type wagerOptions struct {
	messageID   string
	typ         string
	occurredAt  string
	provider    string
	extID       string
	idempotKey  string
	player      string
	wallet      string
	round       string
	game        string
	kind        string
	amount      string
	currency    string
	ref         string
	correlation string
}

// buildWagerEnvelope monta um envelope de operação; messageId vazio é
// regenerado no envio (permite -repeat).
func buildWagerEnvelope(o wagerOptions) (messaging.InboundEnvelope, error) {
	if o.idempotKey == "" {
		o.idempotKey = o.provider + ":" + o.extID
	}
	var occurred time.Time
	if o.occurredAt != "" {
		t, err := time.Parse(time.RFC3339, o.occurredAt)
		if err != nil {
			return messaging.InboundEnvelope{}, fmt.Errorf("producer: -occurred-at inválido (RFC3339): %w", err)
		}
		occurred = t.UTC()
	}
	return messaging.InboundEnvelope{
		MessageID:  o.messageID,
		Type:       o.typ,
		OccurredAt: occurred,
		Data: messaging.InboundWagerData{
			ProviderID:                     o.provider,
			ExternalTransactionID:          o.extID,
			IdempotencyKey:                 o.idempotKey,
			PlayerID:                       o.player,
			WalletID:                       o.wallet,
			RoundID:                        o.round,
			GameID:                         o.game,
			Kind:                           o.kind,
			Money:                          messaging.MoneyJSON{Amount: o.amount, Currency: o.currency},
			ReferenceExternalTransactionID: o.ref,
			CorrelationID:                  o.correlation,
		},
	}, nil
}

// connectAndSend resolve a fila (provisiona se ausente) e enfileira.
func connectAndSend(ctx context.Context, awsCfg config.AWSConfig, envs []messaging.InboundEnvelope, opts sendOptions, level string) (int, error) {
	logger := observability.NewLogger(level)
	client, err := messaging.NewSQSClient(ctx, awsCfg, logger)
	if err != nil {
		return 0, err
	}
	queueURL, err := messaging.ResolveQueueURL(ctx, client, awsCfg.InputQueue)
	if err != nil {
		return 0, err
	}
	sent, err := sendEnvelopes(ctx, client, queueURL, envs, opts, logger)
	if err != nil {
		return 0, err
	}
	fmt.Printf("produzidas %d mensagens em %s\n", sent, queueURL)
	return sent, nil
}

// sampleEnvelope é o exemplo autoritativo do specs.md (seção 10).
const sampleEnvelope = `{
  "messageId": "msg-123",
  "type": "WagerTransactionRequested",
  "occurredAt": "2026-09-08T12:00:00.000Z",
  "data": {
    "providerId": "provider-a",
    "externalTransactionId": "transaction-123",
    "idempotencyKey": "provider-a:transaction-123",
    "playerId": "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1",
    "walletId": "0192f291-27dd-7d3f-8071-5f8685deef37",
    "roundId": "round-987",
    "gameId": "fortune-chimp",
    "kind": "BET",
    "money": { "amount": "25.00", "currency": "BRL" }
  }
}`
