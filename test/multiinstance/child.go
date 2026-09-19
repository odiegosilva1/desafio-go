//go:build integration

// Package multiinstance demonstra as garantias de escala horizontal exigidas
// pelo specs: processos independentes (cada um com suas próprias conexões e
// memória) disputando o mesmo banco. O processo "filho" (TestMain re-exec) roda
// sua própria instância: pool, outbox publisher, reference worker e executor de
// jobs compartilhados via tabelas de controle. As instâncias se coordenam
// apenas pelo banco (FOR UPDATE SKIP LOCKED), como em produção.
package multiinstance

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"desafio-go/internal/application"
	"desafio-go/internal/domain/derr"
	"desafio-go/internal/domain/money"
	"desafio-go/internal/domain/wagering"
	"desafio-go/internal/observability"
	"desafio-go/internal/storage/port"
	"desafio-go/internal/storage/postgres"
)

const (
	envChild    = "MI_CHILD"
	envID       = "MI_ID"
	envDBURL    = "MI_DB_URL"
	envLogDir   = "MI_LOG_DIR"
	envProvider = "MI_PROVIDER"

	// staleAfter define quanto tempo um job reivindicado por uma instância morta
	// fica órfão antes de outra instância assumir (recuperação sem duplicação).
	staleAfter = 8 * time.Second
)

// miJob é um job compartilhado entre as instâncias (tabela mi_jobs).
type miJob struct {
	ID       int64
	Kind     string
	Pld      json.RawMessage
	Attempts int
}

// maxJobAttempts limita as reentregas de um job (análogo ao redrive do SQS).
const maxJobAttempts = 60

// miPayload é o payload JSON de um job.
type miPayload struct {
	Ref     string `json:"ref"`
	Player  string `json:"player"`
	Initial string `json:"initial"`
	Ext     string `json:"ext"`
	Round   string `json:"round"`
	Game    string `json:"game"`
	Kind    string `json:"kind"`
	Amount  string `json:"amount"`
	RefExt  string `json:"refExt"`
}

// sinkPublisher é o destino de eventos do processo filho: cada instância grava
// em SEU próprio log os eventIds que publicou. Republicações preservam o
// eventId (dedup downstream), como o SQS com DeduplicationId.
type sinkPublisher struct {
	id int
	mu sync.Mutex
	f  *os.File
}

func newSinkPublisher(id int, logDir string) (*sinkPublisher, error) {
	f, err := os.OpenFile(logDir+"/publisher."+fmt.Sprint(id)+".log",
		os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &sinkPublisher{id: id, f: f}, nil
}

// Publish registra o eventId no log desta instância e "publica" com sucesso.
func (p *sinkPublisher) Publish(_ context.Context, r port.OutboxRecord) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, err := fmt.Fprintln(p.f, r.EventID)
	return err
}

// childMain roda dentro do binário de teste re-executado (os/exec): é um
// processo OPERACIONAL independente, com pool de conexões, memória, publisher e
// worker próprios, como uma instância de produção. Encerra via job "exit"
// (os.Exit(0)) ou por SIGKILL do orquestrador.
func childMain() {
	id := atoiEnv(envID)
	dbURL := os.Getenv(envDBURL)
	logDir := os.Getenv(envLogDir)
	provider := os.Getenv(envProvider)

	ctx := context.Background()
	pool, err := postgres.NewPool(ctx, dbURL)
	if err != nil {
		fatalChild(id, "pool: %v", err)
	}
	defer pool.Close()

	repos := application.Repos{
		Wallets:  postgres.NewWalletStore(),
		Ledger:   postgres.NewLedgerStore(),
		Wagering: postgres.NewWageringStore(),
		Outbox:   postgres.NewOutboxStore(),
		Inbox:    postgres.NewInboxStore(),
		UOW:      postgres.NewUnitOfWork(pool),
	}
	logger := observability.NewLogger("error")
	svc := application.NewService(repos, logger, observability.NewMetrics())

	pub, err := newSinkPublisher(id, logDir)
	if err != nil {
		fatalChild(id, "sink publisher: %v", err)
	}

	workCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go heartbeatLoop(workCtx, pool, id)

	go func() {
		_ = application.NewOutboxPublisher(
			svc, pub, 40*time.Millisecond, 32, fmt.Sprintf("mi-%d", id)).Run(workCtx)
	}()

	go func() {
		_ = application.NewReferenceWorker(svc, 60*time.Millisecond).Run(workCtx)
	}()

	runExecutor(workCtx, pool, svc, id, provider)
	cancel()
	pool.Close()
	os.Exit(0)
}

// heartbeatLoop mantém a linha de mi_children desta instância atualizada.
func heartbeatLoop(ctx context.Context, pool *pgxpool.Pool, id int) {
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			_, _ = pool.Exec(context.Background(), `
INSERT INTO mi_children (id, hb) VALUES ($1, now())
ON CONFLICT (id) DO UPDATE SET hb = now()`, id)
		}
	}
}

// runExecutor consome jobs de mi_jobs com disputa não bloqueante; uma instância
// morta entre a reivindicação e a conclusão deixa o job órfão, retomado por
// outra após staleAfter (recuperação sem duplicação graças à idempotência).
func runExecutor(ctx context.Context, pool *pgxpool.Pool, svc *application.Service, id int, provider string) {
	for {
		if ctx.Err() != nil {
			return
		}
		job, ok, err := claimJob(ctx, pool, id)
		if err != nil {
			fmt.Fprintf(os.Stderr, "child %d claim: %v\n", id, err)
			time.Sleep(50 * time.Millisecond)
			continue
		}
		if !ok {
			time.Sleep(30 * time.Millisecond)
			continue
		}

		result, execErr := executeJob(ctx, pool, svc, provider, job)
		if execErr != nil {
			// Redelivery: erros de sistema (impasse, escrita concorrente,
			// infraestrutura) não são terminais. Como em produção — o SQS
			// reentrega a mensagem até o limite — o job volta à fila com
			// tentativas limitadas; a idempotência evita duplicação.
			if job.Attempts < maxJobAttempts {
				requeueJob(ctx, pool, job.ID)
				time.Sleep(40 * time.Millisecond)
				continue
			}
		}
		markDone(ctx, pool, job.ID, result, execErr)
		if job.Kind == "exit" {
			return
		}
	}
}

// claimJob reivindica o próximo job disponível (ou órfão vencido) de forma
// atômica e não bloqueante.
func claimJob(ctx context.Context, pool *pgxpool.Pool, id int) (miJob, bool, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return miJob{}, false, err
	}
	defer tx.Rollback(ctx)

	var j miJob
	var payload string
	err = tx.QueryRow(ctx, `
WITH claimed AS (
	SELECT id
	  FROM mi_jobs
	 WHERE done = false
	   AND (claimed_by IS NULL OR claimed_at < now() - interval '8 seconds')
	 ORDER BY id
	 LIMIT 1
	   FOR UPDATE SKIP LOCKED
)
UPDATE mi_jobs SET claimed_by = $1, claimed_at = now()
  FROM claimed
 WHERE mi_jobs.id = claimed.id
 RETURNING mi_jobs.id, mi_jobs.kind, mi_jobs.payload::text, mi_jobs.attempts`, fmt.Sprintf("mi-%d", id)).Scan(&j.ID, &j.Kind, &payload, &j.Attempts)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return miJob{}, false, nil
		}
		return miJob{}, false, err
	}
	j.Pld = json.RawMessage(payload)
	return j, true, tx.Commit(ctx)
}

// requeueJob devolve o job à fila após falha transitória, liberando a
// reivindicação para que qualquer instância o assuma de novo.
func requeueJob(ctx context.Context, pool *pgxpool.Pool, id int64) {
	_, _ = pool.Exec(ctx, `
UPDATE mi_jobs
   SET claimed_by = NULL, claimed_at = NULL, attempts = attempts + 1
 WHERE id = $1`, id)
}

// markDone conclui o job com o resultado (JSON) ou erro.
func markDone(ctx context.Context, pool *pgxpool.Pool, id int64, result []byte, execErr error) {
	var res any
	if result != nil {
		res = string(result)
	} else {
		res = nil
	}
	var errTxt *string
	if execErr != nil {
		s := execErr.Error()
		errTxt = &s
	}
	_, _ = pool.Exec(ctx, `
UPDATE mi_jobs
   SET done = true, result = $2::jsonb, error = $3, finished_at = now()
 WHERE id = $1`, id, res, errTxt)
}

// executeJob despacha um job para o caso de uso da própria instância.
func executeJob(ctx context.Context, pool *pgxpool.Pool, svc *application.Service, provider string, j miJob) ([]byte, error) {
	var p miPayload
	if err := json.Unmarshal(j.Pld, &p); err != nil {
		return nil, err
	}
	switch j.Kind {
	case "open":
		return execOpen(ctx, pool, svc, provider, p)
	case "process":
		return execProcess(ctx, pool, svc, provider, p)
	case "exit":
		return nil, nil
	default:
		return nil, fmt.Errorf("kind desconhecido %q", j.Kind)
	}
}

func execOpen(ctx context.Context, pool *pgxpool.Pool, svc *application.Service, provider string, p miPayload) ([]byte, error) {
	bal, err := money.FromDecimalString(p.Initial, money.CurrencyBRL)
	if err != nil {
		return nil, err
	}
	res, err := svc.OpenWallet(ctx, application.OpenWalletInput{
		ProviderID:     provider,
		PlayerID:       p.Player,
		InitialBalance: bal,
		OccurredAt:     time.Now().UTC(),
	})
	if err != nil {
		if derr.IsClass(err, derr.ClassConflict) {
			// Replay de um job órfão: a carteira já existe; conclude idempotente.
			var walletID, player string
			if qerr := pool.QueryRow(ctx, `
SELECT wallet_id, player FROM mi_wallet_map WHERE ref = $1`, p.Ref).Scan(&walletID, &player); qerr == nil {
				return json.Marshal(map[string]any{"wallet": walletID, "player": player, "replay": true})
			}
		}
		return nil, err
	}
	// Replay-safe: reexecução de um job órfão não cria carteira duplicada.
	_, err = pool.Exec(ctx, `
INSERT INTO mi_wallet_map (ref, wallet_id, player, provider)
VALUES ($1, $2, $3, $4)
ON CONFLICT (ref) DO UPDATE SET wallet_id = $2, player = $3, provider = $4`,
		p.Ref, res.ID, res.PlayerID, provider)
	if err != nil {
		return nil, err
	}
	return json.Marshal(map[string]any{"wallet": res.ID, "balance": res.Balance.Amount(), "version": res.Version})
}

func execProcess(ctx context.Context, pool *pgxpool.Pool, svc *application.Service, provider string, p miPayload) ([]byte, error) {
	var walletID, player string
	err := pool.QueryRow(ctx, `
SELECT wallet_id, player FROM mi_wallet_map WHERE ref = $1`, p.Ref).Scan(&walletID, &player)
	if err != nil {
		return nil, fmt.Errorf("carteira %q não mapeada: %w", p.Ref, err)
	}

	amt, err := money.FromDecimalString(p.Amount, money.CurrencyBRL)
	if err != nil {
		return nil, err
	}
	res, err := svc.Process(ctx, application.ProcessInput{
		ProviderID:     provider,
		ExternalTxID:   p.Ext,
		PlayerID:       player,
		WalletID:       walletID,
		RoundID:        p.Round,
		GameID:         p.Game,
		Kind:           wagering.Kind(p.Kind),
		Money:          amt,
		ReferenceExtID: p.RefExt,
		IdempotencyKey: provider + ":" + p.Ext,
		CorrelationID:  "mi-job-" + fmt.Sprint(p.Ext),
		OccurredAt:     time.Now().UTC(),
	})
	if err != nil {
		return nil, err
	}
	out := map[string]any{
		"status":  string(res.Status),
		"replay":  res.IdempotentReplay,
		"failure": res.FailureCode,
		"tx":      res.TransactionID,
	}
	if res.Balance.Amount() != "" {
		out["balance"] = res.Balance.Amount()
	}
	return json.Marshal(out)
}

func atoiEnv(key string) int {
	var n int
	if _, err := fmt.Sscanf(os.Getenv(key), "%d", &n); err != nil {
		fatalChild(-1, "env %s inválido: %v", key, err)
	}
	return n
}

func fatalChild(id int, format string, args ...any) {
	fmt.Fprintf(os.Stderr, "child %d fatal: "+format+"\n", append([]any{id}, args...)...)
	os.Exit(1)
}
