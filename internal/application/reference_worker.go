package application

import (
	"context"
	"time"

	"desafio-go/internal/observability"
	"desafio-go/internal/storage/port"
)

// ReferenceWorker retoma de forma durável as operações em PENDING_REFERENCE:
// a cada passada tenta resolver a referência; quando disponível, reaplica a
// reversão; caso contrário re-agenda com backoff exponencial. O TTL de
// tentativas (MaxReferenceAttempts) finaliza como REJECTED. Instâncias
// independentes disputam as linhas com FOR UPDATE SKIP LOCKED.
type ReferenceWorker struct {
	service  *Service
	interval time.Duration
}

// NewReferenceWorker constrói o worker de referências pendentes.
func NewReferenceWorker(service *Service, interval time.Duration) *ReferenceWorker {
	return &ReferenceWorker{service: service, interval: interval}
}

// Run executa o worker até o cancelamento do contexto.
func (w *ReferenceWorker) Run(ctx context.Context) error {
	observability.Info(ctx, w.service.logger, "reference worker started")
	defer observability.Info(ctx, w.service.logger, "reference worker stopped")

	if err := w.scan(ctx); err != nil {
		observability.Warn(ctx, w.service.logger, "reference worker initial scan failed", "error", err.Error())
	}

	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := w.scan(ctx); err != nil {
				observability.Warn(ctx, w.service.logger, "reference worker scan failed", "error", err.Error())
			}
		}
	}
}

// scan disputa as referências vencidas e resolve cada uma dentro de sua própria
// transação. O lock FOR UPDATE SKIP LOCKED garante que um item pendente seja
// assumido por exatamente uma instância por passada.
func (w *ReferenceWorker) scan(ctx context.Context) error {
	err := w.service.run(ctx, func(ctx context.Context, tx port.TxScope) error {
		now := time.Now().UTC()
		items, err := w.service.repos.Wagering.ListPendingReference(ctx, tx, now)
		if err != nil {
			return err
		}
		for _, item := range items {
			w.service.metrics.Inc(observability.MetricRefAttempts)
			outcome, err := w.service.attempt(ctx, tx, item, now)
			if err != nil {
				return err
			}
			if outcome.result.Status != "" {
				w.service.metrics.Inc(observability.MetricWagerResults,
					"status", string(outcome.result.Status), "worker", "reference")
			}
		}
		return nil
	})
	return err
}
