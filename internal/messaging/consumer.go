package messaging

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"desafio-go/internal/application"
	"desafio-go/internal/domain/derr"
	"desafio-go/internal/domain/money"
	"desafio-go/internal/domain/wagering"
	"desafio-go/internal/observability"
	"desafio-go/internal/storage/port"
)

// consumerName identifica a inbox do consumidor de operações SQS.
const consumerName = "sqs-wager-transactions"

// MoneyJSON é o formato monetário no corpo da mensagem.
type MoneyJSON struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

// inboundEnvelope é o corpo das mensagens recebidas da fila de operações.
type inboundEnvelope struct {
	MessageID  string           `json:"messageId"`
	Type       string           `json:"type"`
	OccurredAt time.Time        `json:"occurredAt"`
	Data       inboundWagerData `json:"data"`
}

type inboundWagerData struct {
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

// Consumer consome a fila wager-transactions.fifo com a garantia de que o
// registro da inbox e o tratamento durável compartilham a mesma transação. A
// mensagem é removida somente após o commit.
type Consumer struct {
	client       SQSClient
	queueURL     string
	dlqURL       string
	consumerName string
	pollInterval time.Duration
	logger       *slog.Logger
	metrics      *observability.Metrics
	service      *application.Service
	repos        application.Repos
}

// NewConsumer constrói o consumidor SQS.
func NewConsumer(
	client SQSClient,
	queueURL, dlqURL string,
	service *application.Service,
	repos application.Repos,
	logger *slog.Logger,
	metrics *observability.Metrics,
	pollInterval time.Duration,
) *Consumer {
	if pollInterval <= 0 {
		pollInterval = 5 * time.Second
	}
	return &Consumer{
		client:       client,
		queueURL:     queueURL,
		dlqURL:       dlqURL,
		consumerName: consumerName,
		pollInterval: pollInterval,
		logger:       logger,
		metrics:      metrics,
		service:      service,
		repos:        repos,
	}
}

// Run poli a fila em long polling até o cancelamento do contexto. No SIGTERM,
// para de buscar trabalho e conclui o processamento em andamento; a visibilidade
// é liberada automaticamente pelo broker para reentrega segura caso o processo
// caia no meio do tratamento.
func (c *Consumer) Run(ctx context.Context) error {
	observability.Info(context.Background(), c.logger, "sqs consumer started",
		"consumerName", c.consumerName)
	defer observability.Info(context.Background(), c.logger, "sqs consumer stopped")

	for {
		out, err := c.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:              aws.String(c.queueURL),
			MaxNumberOfMessages:   10,
			WaitTimeSeconds:       15,
			VisibilityTimeout:     VisibilityTimeout,
			MessageAttributeNames: []string{"All"},
		})
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			observability.Warn(ctx, c.logger, "sqs receive failed", "error", err.Error())
			time.Sleep(c.pollInterval)
			continue
		}

		for _, msg := range out.Messages {
			if msg.Body == nil || msg.MessageId == nil {
				continue
			}
			c.handleMessage(ctx, msg)
		}

		if ctx.Err() != nil {
			return nil
		}
		time.Sleep(c.pollInterval)
	}
}

// handleMessage trata uma mensagem; retorna quando o tratamento foi concluído
// durablemente (commit) e a remoção da fila é segura.
func (c *Consumer) handleMessage(ctx context.Context, msg types.Message) {
	body := aws.ToString(msg.Body)
	messageID := aws.ToString(msg.MessageId)
	tctx := observability.WithTrace(ctx, observability.Trace{MessageID: messageID})
	hash := sha256Sum(body)

	envelope, err := parseEnvelope(body)
	if err != nil {
		// Mensagem inválida: falha permanente, não corrigível por retry → DLQ.
		observability.Warn(tctx, c.logger, "invalid sqs message", "error", err.Error())
		c.metrics.Inc(observability.MetricSQSDLQ, "reason", "invalid_message")
		c.sendToDLQ(tctx, body, "INVALID_MESSAGE")
		c.delete(tctx, msg)
		return
	}

	trx := observability.TraceFrom(tctx)
	trx.ProviderID = envelope.Data.ProviderID
	trx.TransactionID = envelope.Data.ExternalTransactionID
	tctx = observability.WithTrace(tctx, trx)

	err = c.repos.UOW.Run(tctx, func(ctx context.Context, tx port.TxScope) error {
		first, err := c.repos.Inbox.InsertNew(ctx, tx, c.consumerName, messageID, hash)
		if err != nil {
			return derr.Wrap(derr.ClassTransient, derr.CodeTransient, err)
		}
		if !first {
			dup := c.applyInboxDeduplication(ctx, tx, messageID, hash)
			c.metrics.Inc(observability.MetricInboxDup)
			return dup
		}

		input, err := toProcessInput(envelope)
		if err != nil {
			// Entrada estruturalmente válida, porém fora das regras de domínio:
			// rejeição definitiva (termina sem efeito financeiro).
			c.metrics.Inc(observability.MetricSQSDLQ, "reason", "invalid_payload")
			_ = c.repos.Inbox.Complete(ctx, tx, c.consumerName, messageID, "FAILED", derr.CodeOf(err))
			c.sendToDLQ(ctx, body, derr.CodeOf(err))
			return nil
		}

		result, perr := c.service.ProcessInTx(ctx, tx, input)
		if perr != nil {
			observability.Info(ctx, c.logger, "message processing concluded with error",
				"error", perr.Error(), "code", derr.CodeOf(perr))
			switch {
			case derr.IsClass(perr, derr.ClassPermanent):
				c.metrics.Inc(observability.MetricSQSDLQ, "reason", derr.CodeOf(perr))
				_ = c.repos.Inbox.Complete(ctx, tx, c.consumerName, messageID, "FAILED", derr.CodeOf(perr))
				c.sendToDLQ(ctx, body, derr.CodeOf(perr))
				return nil
			case derr.IsClass(perr, derr.ClassTransient):
				return perr
			default:
				// Conflito de idempotência ou rejeição definitiva já encerrada:
				// mensagem consumida sem novo efeito financeiro.
				_ = c.repos.Inbox.Complete(ctx, tx, c.consumerName, messageID, "PROCESSED", derr.CodeOf(perr))
				return nil
			}
		}
		_ = c.repos.Inbox.Complete(ctx, tx, c.consumerName, messageID, "PROCESSED", result.FailureCode)
		return nil
	})

	if err != nil {
		// Transitória: mensagem permanece na fila; o visibility timeout libera a
		// reentrega e o retry com backoff acontece no próximo recebimento.
		observability.Warn(tctx, c.logger, "transient sqs processing failure",
			"error", err.Error())
		c.metrics.Inc(observability.MetricSQSProcessed, "status", "transient_retry")
		return
	}

	c.delete(tctx, msg)
	c.metrics.Inc(observability.MetricSQSProcessed, "status", "processed")
	observability.Info(tctx, c.logger, "sqs message processed and removed")
}

// applyInboxDeduplication decide a ação para mensagens já registradas
// (reentrega). Verifica o hash da mensagem e conclui conforme o estado salvo.
func (c *Consumer) applyInboxDeduplication(ctx context.Context, tx port.TxScope, messageID, hash string) error {
	status, storedHash, found, err := c.repos.Inbox.Lookup(ctx, tx, c.consumerName, messageID)
	if err != nil || !found {
		return derr.Wrap(derr.ClassTransient, derr.CodeTransient, fmt.Errorf("messaging: inbox lookup failed"))
	}
	if storedHash != hash {
		// Reentrega com conteúdo diferente: conteúdo corrompido — falha terminal.
		_ = c.repos.Inbox.Complete(ctx, tx, c.consumerName, messageID, "FAILED", "PAYLOAD_MISMATCH")
		c.metrics.Inc(observability.MetricSQSDLQ, "reason", "payload_mismatch")
		return nil
	}
	if status == "PROCESSED" {
		c.metrics.Inc(observability.MetricDuplicates, "channel", "sqs")
		return nil
	}
	return nil
}

func (c *Consumer) sendToDLQ(ctx context.Context, body, reason string) {
	_, err := c.client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:       aws.String(c.dlqURL),
		MessageBody:    aws.String(body),
		MessageGroupId: aws.String("wager-dlq"),
		MessageAttributes: map[string]types.MessageAttributeValue{
			"FailureCode": {DataType: aws.String("String"), StringValue: aws.String(reason)},
		},
	})
	if err != nil {
		observability.Warn(ctx, c.logger, "failed to send message to dlq", "error", err.Error())
	}
}

func (c *Consumer) delete(ctx context.Context, msg types.Message) {
	if _, err := c.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(c.queueURL),
		ReceiptHandle: msg.ReceiptHandle,
	}); err != nil {
		observability.Warn(ctx, c.logger, "failed to delete message",
			"messageId", aws.ToString(msg.MessageId), "error", err.Error())
	}
}

func sha256Sum(body string) string {
	sum := sha256.Sum256([]byte(body))
	return fmt.Sprintf("%x", sum)
}

func parseEnvelope(body string) (inboundEnvelope, error) {
	var env inboundEnvelope
	if err := json.Unmarshal([]byte(body), &env); err != nil {
		return env, fmt.Errorf("messaging: malformed envelope: %w", err)
	}
	if env.MessageID == "" || env.Data.ExternalTransactionID == "" {
		return env, fmt.Errorf("messaging: missing messageId/externalTransactionId")
	}
	return env, nil
}

// toProcessInput converte o envelope em ProcessInput do caso de uso. Erros
// indicam entrada inválida (validações de domínio equivalentes ao HTTP).
func toProcessInput(env inboundEnvelope) (wagering.Transaction, error) {
	d := env.Data
	if d.IdempotencyKey == "" {
		return wagering.Transaction{}, derr.ErrMissingIdempotencyKey
	}
	kind := wagering.Kind(d.Kind)
	m, err := money.FromDecimalString(d.Money.Amount, money.Currency(d.Money.Currency))
	if err != nil {
		return wagering.Transaction{}, derr.Wrap(derr.ClassInvalidInput, derr.CodeInvalidMoney, err)
	}
	correlationID := d.CorrelationID
	if correlationID == "" {
		correlationID = env.MessageID
	}
	return application.NewPendingTransaction(application.ProcessInput{
		ProviderID:     d.ProviderID,
		ExternalTxID:   d.ExternalTransactionID,
		PlayerID:       d.PlayerID,
		WalletID:       d.WalletID,
		RoundID:        d.RoundID,
		GameID:         d.GameID,
		Kind:           kind,
		Money:          m,
		ReferenceExtID: d.ReferenceExternalTransactionID,
		IdempotencyKey: d.IdempotencyKey,
		CorrelationID:  correlationID,
		CausationID:    env.MessageID,
		OccurredAt:     env.OccurredAt,
	})
}
