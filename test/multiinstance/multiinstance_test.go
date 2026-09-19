//go:build integration

package multiinstance

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"desafio-go/internal/application"
	"desafio-go/internal/domain/wagering"
	"desafio-go/internal/observability"
	"desafio-go/internal/storage/postgres"
)

// TestMain interrompe a execução do teste quando o binário é re-executado por
// uma instância (os/exec): o processo filho atende a childMain e encerra sem
// rodar a suíte. O orquestrador segue normalmente.
func TestMain(m *testing.M) {
	if os.Getenv(envChild) == "1" {
		childMain()
		os.Exit(0)
	}
	os.Exit(m.Run())
}

const (
	miProvider = "mi-provider"
	miChildren = 3
)

// TestMultiInstanceIndependenceAndRecovery demonstra as garantias de múltiplas
// instâncias independentes (specs, linhas 200/415):
//
//  1. Carteiras paralelas avançam independentemente em processos distintos;
//  2. A mesma carteira é serializada pelo lock (sem perda de atualização),
//     mesmo sob concorrência simultânea de 3 processos;
//  3. Rejeições por saldo insuficiente não movimentam nada;
//  4. A transactional outbox é publicada exatamente uma vez por eventId no
//     agregado das instâncias, mesmo com uma instância morta no meio;
//  5. O reference worker de outra instância assume trabalho abandonado
//     (PENDING_REFERENCE) quando a instância que o iniciou sai do ar.
func TestMultiInstanceIndependenceAndRecovery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()

	pool, logDir := miSetup(t)
	defer pool.Close()

	svc := newService(pool)
	children := startChildren(t, logDir)
	waited := make([]bool, miChildren)
	defer func() {
		for i := 0; i < miChildren; i++ {
			if waited[i] {
				continue
			}
			_ = children[i].Process.Kill()
			_, _ = children[i].Process.Wait()
		}
	}()

	waitReadiness(ctx, t, pool)

	exp := map[string]*expected{}

	// 1ª onda: abertura de carteiras (uma por instância).
	enqueueOpen(t, pool, "pw-a", "pa", "1000.00", exp)
	enqueueOpen(t, pool, "pw-b", "pb", "500.00", exp)
	enqueueOpen(t, pool, "pw-c", "pc", "0.00", exp)
	enqueueOpen(t, pool, "pw-s", "ps", "1000.00", exp)
	enqueueOpen(t, pool, "pw-ref", "pr", "100.00", exp)
	waitJobsDone(ctx, t, pool)

	// 2ª onda: paralelismo entre carteiras distintas + rejeição por saldo.
	enqueueProcess(t, pool, "pw-a", "a-1", "BET", "25.00", "")
	enqueueProcess(t, pool, "pw-a", "a-2", "BET", "10.00", "")
	enqueueProcess(t, pool, "pw-a", "a-3", "WIN", "50.00", "")
	enqueueProcess(t, pool, "pw-b", "b-1", "BET", "100.00", "")
	enqueueProcess(t, pool, "pw-b", "b-2", "WIN", "200.00", "")
	enqueueProcess(t, pool, "pw-c", "c-1", "BET", "10.00", "")
	waitJobsDone(ctx, t, pool)
	settleOutbox(ctx, t, pool)

	exp["pw-a"].balance = "1015.00" // open +1000, -25, -10, +50
	exp["pw-a"].entries = 4
	exp["pw-b"].balance = "600.00" // open +500, -100, +200
	exp["pw-b"].entries = 3
	exp["pw-c"].balance = "0.00"
	exp["pw-c"].entries = 0
	exp["pw-c"].rejectedExts = map[string]string{"c-1": "INSUFFICIENT_FUNDS"}
	// Ainda sem movimentações: apenas a abertura de cada uma.
	exp["pw-s"].balance = "1000.00"
	exp["pw-s"].entries = 1
	exp["pw-ref"].balance = "100.00"
	exp["pw-ref"].entries = 1
	assertWallets(ctx, t, pool, svc, exp)

	// 3ª onda: 12 apostas simultâneas na MESMA carteira. Cada uma disputa o
	// lock FOR UPDATE do wallet; o resultado nunca perde atualização.
	for i := 1; i <= 12; i++ {
		enqueueProcess(t, pool, "pw-s", fmt.Sprintf("s%d", i), "BET", "10.00", "")
	}
	waitJobsDone(ctx, t, pool)
	settleOutbox(ctx, t, pool)
	exp["pw-s"].balance = "880.00" // 1000 - 12*10
	exp["pw-s"].entries = 13       // openamento + 12 apostas
	assertWallets(ctx, t, pool, svc, exp)

	// --- Falha de instância + retomada de trabalho abandonado --------------
	// A instância 0 inicia um REFUND cuja referência (BET) ainda não existe:
	// vira PENDING_REFERENCE durável; nenhuma carteira é movimentada.
	enqueueProcess(t, pool, "pw-ref", "rf-1", "REFUND", "30.00", "bet-orig")
	waitJobsDone(ctx, t, pool)
	assertState(ctx, t, pool, miProvider, "rf-1", wagering.StatePendingReference)

	// Morte brutal da instância 0: conexões encerradas, locks liberados e
	// trabalhos não confirmados descartados pelo rollback. Jobs órfãos da
	// outbox permanecem PENDING para as sobreviventes assumirem.
	if err := children[0].Process.Kill(); err != nil {
		t.Fatalf("kill child 0: %v", err)
	}
	_ = children[0].Wait()
	waited[0] = true

	// As instâncias sobreviventes concluem a operação-base da referência...
	enqueueProcess(t, pool, "pw-ref", "bet-orig", "BET", "30.00", "")
	waitJobsDone(ctx, t, pool)

	// ...e o reference worker de UMA delas assume a PENDING_REFERENCE órfã,
	// completando a reversão sem duplicação.
	waitFor(ctx, t, 20*time.Second, "retomada da PENDING_REFERENCE", func() bool {
		return stateOf(ctx, pool, miProvider, "rf-1") == wagering.StateProcessed
	})
	settleOutbox(ctx, t, pool)
	exp["pw-ref"].balance = "100.00" // open +100, BET -30, refund +30
	exp["pw-ref"].entries = 3
	assertWallets(ctx, t, pool, svc, exp)

	// --- Garantias globais da publicação -------------------------------
	assertOutboxExactlyOnce(ctx, t, pool, logDir)

	// --- Encerramento ordenado das instâncias sobreviventes -----------------
	for i := 1; i < miChildren; i++ {
		enqueueExit(t, pool)
	}
	for i := 1; i < miChildren; i++ {
		if err := children[i].Wait(); err != nil {
			t.Fatalf("child %d exit: %v", i, err)
		}
		waited[i] = true
	}
}

// --- setup do banco compartilhado --------------------------------------------

func miSetup(t *testing.T) (*pgxpool.Pool, string) {
	t.Helper()
	ctx := context.Background()
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		url = "postgres://wallet:wallet@localhost:5432/wallet?sslmode=disable"
	}
	pool, err := postgres.NewPool(ctx, url)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}

	mig := postgres.NewMigrator(pool, os.DirFS("../../internal/app/migrations"))
	for {
		v, err := mig.Down(ctx)
		if err != nil {
			t.Fatalf("down cleanup: %v", err)
		}
		if v == 0 {
			break
		}
	}
	if err := mig.Up(ctx); err != nil {
		t.Fatalf("up: %v", err)
	}

	for _, ddl := range []string{
		`DROP TABLE IF EXISTS mi_jobs, mi_children, mi_wallet_map CASCADE`,
		`CREATE TABLE mi_jobs (
			id bigserial PRIMARY KEY,
			kind text NOT NULL,
			payload jsonb NOT NULL DEFAULT '{}',
			attempts integer NOT NULL DEFAULT 0,
			claimed_by text,
			claimed_at timestamptz,
			done boolean NOT NULL DEFAULT false,
			result jsonb,
			error text,
			created_at timestamptz NOT NULL DEFAULT now(),
			finished_at timestamptz
		)`,
		`CREATE TABLE mi_children (
			id integer PRIMARY KEY,
			hb timestamptz NOT NULL DEFAULT now()
		)`,
		`CREATE TABLE mi_wallet_map (
			ref text PRIMARY KEY,
			wallet_id text NOT NULL,
			player text NOT NULL,
			provider text NOT NULL,
			created_at timestamptz NOT NULL DEFAULT now()
		)`,
	} {
		if _, err := pool.Exec(ctx, ddl); err != nil {
			t.Fatalf("control ddl: %v", err)
		}
	}
	return pool, t.TempDir()
}

func newService(pool *pgxpool.Pool) *application.Service {
	return application.NewService(application.Repos{
		Wallets:  postgres.NewWalletStore(),
		Ledger:   postgres.NewLedgerStore(),
		Wagering: postgres.NewWageringStore(),
		Outbox:   postgres.NewOutboxStore(),
		Inbox:    postgres.NewInboxStore(),
		UOW:      postgres.NewUnitOfWork(pool),
	}, observability.NewLogger("error"), observability.NewMetrics())
}

// --- instâncias (processos independentes) -------------------------------------

func startChildren(t *testing.T, logDir string) []*exec.Cmd {
	t.Helper()
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		url = "postgres://wallet:wallet@localhost:5432/wallet?sslmode=disable"
	}
	cmds := make([]*exec.Cmd, miChildren)
	for i := 0; i < miChildren; i++ {
		cmd := exec.Command(os.Args[0], "-test.run=^$", "-test.timeout=120s")
		cmd.Env = append(os.Environ(),
			envChild+"=1",
			fmt.Sprintf("%s=%d", envID, i),
			envDBURL+"="+url,
			envLogDir+"="+logDir,
			envProvider+"="+miProvider,
		)
		cmd.Stdout = io.Discard
		cmd.Stderr = os.Stderr
		if err := cmd.Start(); err != nil {
			t.Fatalf("start child %d: %v", i, err)
		}
		cmds[i] = cmd
	}
	return cmds
}

func waitReadiness(ctx context.Context, t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	waitFor(ctx, t, 30*time.Second, "readiness das instâncias", func() bool {
		var n int
		if err := pool.QueryRow(ctx, `
SELECT count(*) FROM mi_children WHERE hb > now() - interval '3 seconds'`).Scan(&n); err != nil {
			return false
		}
		return n == miChildren
	})
}

// --- helpers de orquestração ---------------------------------------------------

func enqueue(t *testing.T, pool *pgxpool.Pool, kind string, p miPayload) {
	t.Helper()
	b, err := json.Marshal(p)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	if _, err := pool.Exec(context.Background(), `INSERT INTO mi_jobs (kind, payload) VALUES ($1, $2)`, kind, b); err != nil {
		t.Fatalf("enqueue %s: %v", kind, err)
	}
}

func enqueueOpen(t *testing.T, pool *pgxpool.Pool, ref, player, initial string, exp map[string]*expected) {
	t.Helper()
	enqueue(t, pool, "open", miPayload{Ref: ref, Player: player, Initial: initial})
	exp[ref] = &expected{}
}

func enqueueProcess(t *testing.T, pool *pgxpool.Pool, ref, ext, kind, amount, refExt string) {
	t.Helper()
	enqueue(t, pool, "process", miPayload{
		Ref: ref, Ext: ext, Kind: kind, Amount: amount, RefExt: refExt,
		Round: "r1", Game: "g1",
	})
}

func enqueueExit(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	enqueue(t, pool, "exit", miPayload{})
}

// waitJobsDone espera zero jobs pendentes e nenhuma reivindicação ativa ou
// órfã dentro da janela de staleness.
func waitJobsDone(ctx context.Context, t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	waitFor(ctx, t, 60*time.Second, "conclusão dos jobs", func() bool {
		var open int
		if err := pool.QueryRow(ctx, `
SELECT count(*) FROM mi_jobs
 WHERE NOT done
   AND (claimed_by IS NULL OR claimed_at >= now() - interval '8 seconds')`).Scan(&open); err != nil {
			return false
		}
		return open == 0
	})
}

// settleOutbox espera que toda a outbox gerada no cenário seja publicada.
func settleOutbox(ctx context.Context, t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	waitFor(ctx, t, 30*time.Second, "publicação total da outbox", func() bool {
		var pending int
		if err := pool.QueryRow(ctx, `
SELECT count(*) FROM outbox WHERE status <> 'PUBLISHED'`).Scan(&pending); err != nil {
			return false
		}
		return pending == 0
	})
}

func stateOf(ctx context.Context, pool *pgxpool.Pool, provider, ext string) wagering.State {
	var s string
	_ = pool.QueryRow(ctx, `
SELECT state FROM wagering_transactions
 WHERE provider_id = $1 AND external_tx_id = $2`, provider, ext).Scan(&s)
	return wagering.State(s)
}

func assertState(ctx context.Context, t *testing.T, pool *pgxpool.Pool, provider, ext string, want wagering.State) {
	t.Helper()
	waitFor(ctx, t, 20*time.Second, "estado de "+ext, func() bool {
		return stateOf(ctx, pool, provider, ext) == want
	})
}

// --- assertions ----------------------------------------------------------------

type expected struct {
	balance      string
	entries      int
	rejectedExts map[string]string
}

// assertWallets confere saldo e quantidade de lançamentos de ledger de cada
// carteira do cenário.
func assertWallets(ctx context.Context, t *testing.T, pool *pgxpool.Pool, svc *application.Service, exp map[string]*expected) {
	t.Helper()
	for ref, e := range exp {
		var walletID string
		if err := pool.QueryRow(ctx, `SELECT wallet_id FROM mi_wallet_map WHERE ref = $1`, ref).Scan(&walletID); err != nil {
			t.Fatalf("carteira %s não mapeada: %v", ref, err)
		}

		w, err := svc.GetWallet(ctx, miProvider, walletID)
		if err != nil {
			t.Fatalf("get wallet %s: %v", ref, err)
		}
		if got := w.Balance().Amount(); got != e.balance {
			t.Errorf("saldo(%s): got %s, want %s", ref, got, e.balance)
		}

		var entries int
		if err := pool.QueryRow(ctx, `
SELECT count(*) FROM wallet_ledger WHERE wallet_id = $1`, walletID).Scan(&entries); err != nil {
			t.Fatalf("ledger %s: %v", ref, err)
		}
		if entries != e.entries {
			t.Errorf("ledger(%s): got %d entries, want %d", ref, entries, e.entries)
		}

		for ext, wantFailure := range e.rejectedExts {
			var state, failure string
			err := pool.QueryRow(ctx, `
SELECT state, COALESCE(failure_code, '')
  FROM wagering_transactions
 WHERE provider_id = $1 AND external_tx_id = $2`, miProvider, ext).Scan(&state, &failure)
			if err != nil {
				t.Fatalf("tx %s: %v", ext, err)
			}
			if state != string(wagering.StateRejected) || failure != wantFailure {
				t.Errorf("tx %s: got state=%s failure=%s, want REJECTED/%s", ext, state, failure, wantFailure)
			}
		}
	}
}

// assertOutboxExactlyOnce confere as garantias de publicação distribuída:
//   - toda linha da outbox terminou PUBLISHED por alguma instância;
//   - nenhum eventId aparece duas vezes no log da MESMA instância;
//   - todo eventId publicado aparece no log de ao menos uma instância.
//
// Republicações (publicar e morrer antes do commit) geram o mesmo eventId em
// instâncias distintas — aceito: o dedup downstream é pelo eventId.
func assertOutboxExactlyOnce(ctx context.Context, t *testing.T, pool *pgxpool.Pool, logDir string) {
	t.Helper()
	published := map[string]string{} // eventId -> published_by
	rows, err := pool.Query(ctx, `SELECT id, published_by FROM outbox WHERE status = 'PUBLISHED'`)
	if err != nil {
		t.Fatalf("outbox: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id, by string
		if err := rows.Scan(&id, &by); err != nil {
			t.Fatalf("outbox scan: %v", err)
		}
		published[id] = by
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("outbox rows: %v", err)
	}

	logs := make([]map[string]bool, miChildren)
	total := map[string]int{}
	for i := 0; i < miChildren; i++ {
		logs[i] = readEventLog(t, logDir, i)
		for ev, single := range logs[i] {
			if !single {
				t.Errorf("instância %d: eventId %s apareceu duas vezes", i, ev)
			}
			total[ev]++
		}
	}

	for ev, by := range published {
		if by == "" {
			t.Errorf("evento %s publicado sem published_by", ev)
		}
		if total[ev] == 0 {
			t.Errorf("evento %s não aparece em nenhum log de instância", ev)
		}
	}
}

func readEventLog(t *testing.T, logDir string, id int) map[string]bool {
	t.Helper()
	set := map[string]bool{}
	f, err := os.Open(filepath.Join(logDir, fmt.Sprintf("publisher.%d.log", id)))
	if err != nil {
		t.Fatalf("log da instância %d: %v", id, err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	counts := map[string]int{}
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		counts[line]++
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("read log %d: %v", id, err)
	}
	for ev, n := range counts {
		set[ev] = n == 1
	}
	return set
}

// --- misc ----------------------------------------------------------------------

func waitFor(ctx context.Context, t *testing.T, timeout time.Duration, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if ok() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout aguardando %s", what)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("contexto encerrado aguardando %s: %v", what, ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}
