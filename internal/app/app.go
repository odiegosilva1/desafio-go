// Package app compõe a aplicação com Uber Fx: configuração, conexões,
// repositórios, casos de uso, mensageria, HTTP e workers são montados por
// construtores injetáveis (fx.Provide) e iniciados/encerrados via fx.Lifecycle.
package app

import (
	"context"
	"errors"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"strconv"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"

	"desafio-go/internal/application"
	"desafio-go/internal/auth"
	"desafio-go/internal/config"
	"desafio-go/internal/httpapi"
	"desafio-go/internal/messaging"
	"desafio-go/internal/observability"
	"desafio-go/internal/storage/port"
	"desafio-go/internal/storage/postgres"
)

// runner abstrai os workers com ciclo de vida (Run até cancelamento).
type runner interface {
	Run(ctx context.Context) error
}

// New monta o grafo Fx completo do serviço. A ordem de inicialização segue as
// dependências: pool → migrations → casos de uso → filas → workers → HTTP.
// Opções extras (opts) são anexadas após o módulo e podem sobrescrever
// construtores (ex.: substituir o verificador OIDC em testes).
func New(opts ...fx.Option) *fx.App {
	return fx.New(append([]fx.Option{baseModule()}, opts...)...)
}

func baseModule() fx.Option {
	return fx.Module("wallet-service",
		fx.Provide(
			provideContext,
			newLogger,
			observability.NewMetrics,
			config.Load,
			newPool,
			newAWSConfig,
			fx.Annotate(postgres.NewUnitOfWork, fx.As(new(port.UnitOfWork))),
			fx.Annotate(postgres.NewWalletStore, fx.As(new(port.WalletStore))),
			fx.Annotate(postgres.NewLedgerStore, fx.As(new(port.LedgerStore))),
			fx.Annotate(postgres.NewWageringStore, fx.As(new(port.WageringStore))),
			fx.Annotate(postgres.NewOutboxStore, fx.As(new(port.OutboxStore))),
			fx.Annotate(postgres.NewInboxStore, fx.As(new(port.InboxStore))),
			newRepos,
			application.NewService,
			fx.Annotate(application.NewService, fx.As(new(httpapi.Service))),
			messaging.NewSQSClient,
			messaging.ResolveQueues,
			newEventPublisher,
			newConsumer,
			newOutboxPublisher,
			newReferenceWorker,
			newAuthVerifier,
			httpapi.NewHandlers,
			newReadiness,
			newHTTPHandler,
			newHTTPServer,
		),
		fx.Invoke(
			runMigrations,
			registerLifecycle,
		),
	)
}

func newLogger(cfg config.Config) *slog.Logger {
	return observability.NewLogger(cfg.LogLevel)
}

// newAWSConfig expõe a sub-configuração AWS para os construtores de mensageria.
func newAWSConfig(cfg config.Config) config.AWSConfig {
	return cfg.AWS
}

func newPool(ctx context.Context, cfg config.Config) (*pgxpool.Pool, error) {
	return postgres.NewPool(ctx, cfg.DatabaseURL)
}

// provideContext fornece o contexto raiz do processo. O contexto é cancelado
// apenas quando o processo encerra; o desligamento ordenado dos workers usa um
// contexto derivado criado no registro de lifecycle.
func provideContext() context.Context {
	return context.Background()
}

func newRepos(
	walletStore port.WalletStore,
	ledgerStore port.LedgerStore,
	wageringStore port.WageringStore,
	outboxStore port.OutboxStore,
	inboxStore port.InboxStore,
	uow port.UnitOfWork,
) application.Repos {
	return application.Repos{
		Wallets: walletStore, Ledger: ledgerStore, Wagering: wageringStore,
		Outbox: outboxStore, Inbox: inboxStore, UOW: uow,
	}
}

func newEventPublisher(
	client messaging.SQSClient,
	queues messaging.Queues,
	logger *slog.Logger,
	metrics *observability.Metrics,
) application.OutboundPublisher {
	return messaging.NewEventPublisher(client, queues.EventQueueURL, logger, metrics)
}

// newConsumer adapta o consumidor SQS às filas resolvidas e à configuração.
func newConsumer(
	client messaging.SQSClient,
	queues messaging.Queues,
	service *application.Service,
	repos application.Repos,
	logger *slog.Logger,
	metrics *observability.Metrics,
	cfg config.Config,
) *messaging.Consumer {
	return messaging.NewConsumer(client, queues.InputQueueURL, queues.DLQURL,
		service, repos, logger, metrics, cfg.Workers.SQSPollInterval)
}

func newOutboxPublisher(
	service *application.Service,
	publisher application.OutboundPublisher,
	cfg config.Config,
) *application.OutboxPublisher {
	instance, err := os.Hostname()
	if err != nil || instance == "" {
		instance = "unknown"
	}
	return application.NewOutboxPublisher(
		service, publisher,
		cfg.Workers.OutboxInterval, cfg.Workers.OutboxBatchSize,
		instance+"#"+strconv.Itoa(os.Getpid()),
	)
}

func newReferenceWorker(service *application.Service, cfg config.Config) *application.ReferenceWorker {
	return application.NewReferenceWorker(service, cfg.Workers.ReferenceInterval)
}

func newAuthVerifier(ctx context.Context, cfg config.Config) (auth.Verifier, error) {
	return auth.NewVerifier(ctx, cfg.OIDC.IssuerURL, cfg.OIDC.ClientID, cfg.OIDC.InternalClientID)
}

func newHTTPHandler(
	handlers *httpapi.Handlers,
	verifier auth.Verifier,
	logger *slog.Logger,
	metrics *observability.Metrics,
	ready httpapi.Pinger,
) http.Handler {
	return httpapi.Router{
		Handlers: handlers,
		Verifier: verifier,
		Logger:   logger,
		Metrics:  metrics,
		Ping:     ready,
		Version:  "dev",
	}.Handler()
}

func newHTTPServer(cfg config.Config, handler http.Handler, logger *slog.Logger) *httpapi.Server {
	return httpapi.NewServer(cfg.HTTPAddr, handler, logger)
}

// runMigrations aplica as migrations pendentes na inicialização. Executar as
// migrations no start simplifica o desenvolvimento; em produção pode-se delegar
// a um job dedicado.
func runMigrations(lc fx.Lifecycle, pool *pgxpool.Pool, logger *slog.Logger) {
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			files, err := fs.Sub(migrationsFS, "migrations")
			if err != nil {
				return err
			}
			migrator := postgres.NewMigrator(pool, files)
			if err := migrator.Up(ctx); err != nil {
				return err
			}
			logger.Info("migrations applied")
			return nil
		},
	})
}

// registerLifecycle registra os workers e o servidor HTTP no fx.Lifecycle. O
// OnStop cancela os runners em ordem reversa e aguarda a conclusão dentro do
// prazo de shutdown configurado.
func registerLifecycle(
	lc fx.Lifecycle,
	ctx context.Context,
	logger *slog.Logger,
	cfg config.Config,
	server *httpapi.Server,
	consumer *messaging.Consumer,
	referenceWorker *application.ReferenceWorker,
	outboxPublisher *application.OutboxPublisher,
) {
	runners := []runner{consumer, referenceWorker, outboxPublisher}
	var cancelWorkers context.CancelFunc
	var workerCtx context.Context

	lc.Append(fx.Hook{
		OnStart: func(startCtx context.Context) error {
			workerCtx, cancelWorkers = context.WithCancel(ctx)
			for i, run := range runners {
				r := run
				idx := i + 1
				go func() {
					logger.Info("worker starting", "worker", idx)
					if err := r.Run(workerCtx); err != nil {
						logger.Warn("worker stopped with error", "worker", idx, "error", err.Error())
					}
				}()
			}
			go func() {
				if err := server.Serve(workerCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
					logger.Warn("http server stopped with error", "error", err.Error())
				}
			}()
			if err := server.Listen(); err != nil {
				logger.Warn("http server listen failed", "error", err.Error())
				return err
			}
			return nil
		},
		OnStop: func(stopCtx context.Context) error {
			if cancelWorkers != nil {
				cancelWorkers()
			}
			timeoutCtx, cancel := context.WithTimeout(stopCtx, cfg.ShutdownTimeout)
			defer cancel()
			<-timeoutCtx.Done()
			return nil
		},
	})
}
