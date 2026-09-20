package messaging

import (
	"context"
	"log/slog"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"desafio-go/internal/observability"
	"desafio-go/internal/storage/port"
)

// EventPublisher publica registros da transactional outbox na fila de eventos
// de saída (wallet-events.fifo). RepublficaEventId como MessageDeduplicationId:
// republicações preservam o eventId e o SQS deduplica dentro da janela.
type EventPublisher struct {
	client   SQSClient
	queueURL string
	logger   *slog.Logger
	metrics  *observability.Metrics
}

// NewEventPublisher constrói o publisher de eventos de saída.
func NewEventPublisher(client SQSClient, queueURL string, logger *slog.Logger, metrics *observability.Metrics) *EventPublisher {
	return &EventPublisher{client: client, queueURL: queueURL, logger: logger, metrics: metrics}
}

// Publish envia o snapshot imutável do evento para a fila de saída.
func (p *EventPublisher) Publish(ctx context.Context, record port.OutboxRecord) error {
	_, err := p.client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:               aws.String(p.queueURL),
		MessageBody:            aws.String(string(record.Payload)),
		MessageGroupId:         aws.String(record.AggregateID),
		MessageDeduplicationId: aws.String(record.EventID),
		MessageAttributes: map[string]types.MessageAttributeValue{
			"EventType": {DataType: aws.String("String"), StringValue: aws.String(record.EventType)},
		},
	})
	if err != nil {
		observability.Warn(ctx, p.logger, "event publish failed",
			"eventId", record.EventID, "eventType", record.EventType, "error", err.Error())
		p.metrics.Inc(observability.MetricOutboxRetries, "eventId", record.EventID)
		return err
	}
	p.metrics.Inc(observability.MetricSQSProcessed, "channel", "outbox")
	return nil
}
