package application

import (
	"context"
	"time"

	"desafio-go/internal/observability"
	"desafio-go/internal/storage/port"
)

// OutboundPublisher publica o payload de um registro da outbox no destino de
// eventos. Implementado pela camada de mensageria (SQS).
type OutboundPublisher interface {
	// Publish envia o evento, preservando eventId em republicações.
	Publish(ctx context.Context, record port.OutboxRecord) error
}

// OutboxPublisher lê registros pendentes da transactional outbox e publica no
// destino de eventos, confirmando a publicação (MarkPublished) no mesmo commit
// da disputa. Suporta múltiplos publishers (FOR UPDATE SKIP LOCKED), backoff e
// recuperação de trabalho abandonado: um registro cujo commit falhou após a
// publicação é republicado, preservando o eventId.
type OutboxPublisher struct {
	service   *Service
	publisher OutboundPublisher
	interval  time.Duration
	batch     int
	// instanceID identifica esta instância em published_by.
	instanceID string
}

// NewOutboxPublisher constrói o publisher da transactional outbox.
func NewOutboxPublisher(
	service *Service,
	publisher OutboundPublisher,
	interval time.Duration,
	batch int,
	instanceID string,
) *OutboxPublisher {
	if batch <= 0 {
		batch = 32
	}
	return &OutboxPublisher{
		service:    service,
		publisher:  publisher,
		interval:   interval,
		batch:      batch,
		instanceID: instanceID,
	}
}

// Run executa o publisher até o cancelamento do contexto.
func (p *OutboxPublisher) Run(ctx context.Context) error {
	observability.Info(ctx, p.service.logger, "outbox publisher started")
	defer observability.Info(ctx, p.service.logger, "outbox publisher stopped")

	if err := p.scan(ctx); err != nil {
		observability.Warn(ctx, p.service.logger, "outbox publisher initial scan failed", "error", err.Error())
	}

	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := p.scan(ctx); err != nil {
				observability.Warn(ctx, p.service.logger, "outbox publisher scan failed", "error", err.Error())
			}
		}
	}
}

// scan disputa um lote de registros pendentes e publica cada um. A publicação
// ocorre antes da confirmação (MarkPublished) no mesmo commit: uma falha entre
// a publicação e o commit gera republicação segura, preservando o eventId.
func (p *OutboxPublisher) scan(ctx context.Context) error {
	err := p.service.repos.UOW.Run(ctx, func(ctx context.Context, tx port.TxScope) error {
		now := time.Now().UTC()
		records, err := p.service.repos.Outbox.Claim(ctx, tx, p.batch, now)
		if err != nil {
			return err
		}
		for _, r := range records {
			if err := p.publisher.Publish(ctx, r); err != nil {
				observability.Warn(ctx, p.service.logger, "outbox publish failed",
					"eventId", r.EventID, "error", err.Error())
				p.service.metrics.Inc(observability.MetricOutboxRetries, "eventId", r.EventID)
				return err
			}
			if err := p.service.repos.Outbox.MarkPublished(ctx, tx, r.EventID, p.instanceID, now); err != nil {
				return err
			}
			latency := now.Sub(r.OccurredAt)
			p.service.metrics.Observe(observability.MetricOutboxDelay, latency)
			observability.Info(ctx, p.service.logger, "outbox event published",
				"eventId", r.EventID, "eventType", r.EventType,
				"aggregateId", r.AggregateID)
		}
		return nil
	})
	return err
}
