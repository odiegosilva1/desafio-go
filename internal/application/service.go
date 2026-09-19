// Package application contém os casos de uso que orquestram domínio e
// persistência, compartilhados por HTTP e SQS. O domínio permanece independente
// de Fx, HTTP, SQS e PostgreSQL; esta camada só depende das interfaces de
// internal/storage/port e dos pacotes de domínio.
package application

import (
	"context"
	"errors"
	"log/slog"

	"github.com/google/uuid"

	"desafio-go/internal/domain/derr"
	"desafio-go/internal/observability"
	"desafio-go/internal/storage/port"
)

// Repos agrupa os repositórios de persistência injetados nos casos de uso.
type Repos struct {
	Wallets  port.WalletStore
	Ledger   port.LedgerStore
	Wagering port.WageringStore
	Outbox   port.OutboxStore
	Inbox    port.InboxStore
	UOW      port.UnitOfWork
}

// Service é o front-end dos casos de uso.
type Service struct {
	repos   Repos
	logger  *slog.Logger
	metrics *observability.Metrics
	// newID permite injetar a fonte de identificadores (UUID) nos testes.
	newID func() string

	// maxProcessRetries limita as tentativas de retomada após falha transitória
	// de concorrência detectada na transação.
	maxProcessRetries int
}

// NewService constrói o serviço de aplicação.
func NewService(repos Repos, logger *slog.Logger, metrics *observability.Metrics) *Service {
	return &Service{
		repos:             repos,
		logger:            logger,
		metrics:           metrics,
		newID:             uuid.NewString,
		maxProcessRetries: 5,
	}
}

// WithIDGenerator substitui o gerador de identificadores (uso em testes).
func (s *Service) WithIDGenerator(fn func() string) *Service {
	s.newID = fn
	return s
}

// retryableString marca erros retryáveis por detecção de escrita concorrente
// ou falha de serialização do banco, com retry limitado do caso de uso.
func isRetryableConcurrency(err error) bool {
	if errors.Is(err, port.ErrConcurrentUpdate) {
		return true
	}
	return false
}

// run retry executa fn dentro de uma transação, retentando um número limitado
// de vezes diante de conflitos de concorrência (detecção otimista/serialização).
func (s *Service) run(ctx context.Context, fn func(ctx context.Context, tx port.TxScope) error) error {
	var err error
	for attempt := 0; attempt <= s.maxProcessRetries; attempt++ {
		err = s.repos.UOW.Run(ctx, fn)
		if !isRetryableConcurrency(err) {
			return err
		}
		observability.Warn(ctx, s.logger, "balance update conflict, retrying",
			"attempt", attempt+1, "failureCode", derr.CodeOf(err))
		s.metrics.Inc(observability.MetricConcurrencyConf)
	}
	return err
}
