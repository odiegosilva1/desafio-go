//go:build integration

package postgres

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func integrationPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		url = "postgres://wallet:wallet@localhost:5432/wallet?sslmode=disable"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := NewPool(ctx, url)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func TestMigratorUpDownIntegration(t *testing.T) {
	pool := integrationPool(t)
	ctx := context.Background()
	mig := NewMigrator(pool, os.DirFS("../../app/migrations"))

	// Limpeza defensiva: reverter quantas versões existirem.
	for {
		v, err := mig.Down(ctx)
		if err != nil {
			t.Fatalf("Down (cleanup): %v", err)
		}
		if v == 0 {
			break
		}
	}

	if err := mig.Up(ctx); err != nil {
		t.Fatalf("Up: %v", err)
	}
	for _, table := range []string{
		"wallets", "wallet_ledger", "wagering_transactions",
		"outbox", "inbox", "schema_version",
	} {
		if !tableExists(ctx, pool, table) {
			t.Errorf("table %q not created", table)
		}
	}

	// Up é idempotente (re-aplicar não falha nem duplica).
	if err := mig.Up(ctx); err != nil {
		t.Fatalf("Up (re-run): %v", err)
	}

	v, err := mig.Down(ctx)
	if err != nil {
		t.Fatalf("Down: %v", err)
	}
	if v != 1 {
		t.Fatalf("Down version = %d, want 1", v)
	}
	if tableExists(ctx, pool, "wallets") {
		t.Error("wallets still exists after Down")
	}
}

func TestLedgerSchemaConstraintsIntegration(t *testing.T) {
	pool := integrationPool(t)
	ctx := context.Background()
	mig := NewMigrator(pool, os.DirFS("../../app/migrations"))
	if err := mig.Up(ctx); err != nil {
		t.Fatalf("Up: %v", err)
	}

	if _, err := pool.Exec(ctx, `
TRUNCATE wallet_ledger, wagering_transactions, outbox, inbox, wallets CASCADE`); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	if _, err := pool.Exec(ctx, `
INSERT INTO wallets (id, provider_id, player_id, currency, balance_units, version)
VALUES ('w1', 'provider-a', 'p1', 'BRL', 10000, 1)`); err != nil {
		t.Fatalf("insert wallet: %v", err)
	}

	insertLedger := `
INSERT INTO wallet_ledger (id, wallet_id, transaction_id, direction, amount_units, balance_before, balance_after)
VALUES ($1, 'w1', $2, 'CREDIT', 10000, 0, 10000)`

	if _, err := pool.Exec(ctx, insertLedger, "e1", "t1"); err != nil {
		t.Fatalf("insert ledger: %v", err)
	}

	// Unicidade (wallet_id, transaction_id): lançamento duplicado rejeitado.
	if _, err := pool.Exec(ctx, insertLedger, "e2", "t1"); err == nil {
		t.Error("expected error on duplicate (wallet_id, transaction_id)")
	}

	// Imutabilidade do ledger: UPDATE/DELETE bloqueados pelo trigger.
	if _, err := pool.Exec(ctx, `UPDATE wallet_ledger SET amount_units = 1 WHERE id = 'e1'`); err == nil {
		t.Error("expected error on UPDATE of immutable ledger")
	}
	if _, err := pool.Exec(ctx, `DELETE FROM wallet_ledger WHERE id = 'e1'`); err == nil {
		t.Error("expected error on DELETE of immutable ledger")
	}

	// Continuidade de saldo violada: balance_after inválido.
	if _, err := pool.Exec(ctx, `
INSERT INTO wallet_ledger (id, wallet_id, transaction_id, direction, amount_units, balance_before, balance_after)
VALUES ('e3', 'w1', 't3', 'DEBIT', 9999, 0, 9999)`); err == nil {
		t.Error("expected error on balance mismatch")
	}

	// Saldo de carteira nunca negativo.
	if _, err := pool.Exec(ctx, `UPDATE wallets SET balance_units = -1 WHERE id = 'w1'`); err == nil {
		t.Error("expected error on negative wallet balance")
	}

	// Dupla carteira no mesmo (provider, player, currency) é conflito.
	if _, err := pool.Exec(ctx, `
INSERT INTO wallets (id, provider_id, player_id, currency) VALUES ('w2', 'provider-a', 'p1', 'BRL')`); err == nil {
		t.Error("expected error on duplicate provider/player/currency wallet")
	}

	// Carteira de outro provedor para o mesmo jogador é permitida (isolamento).
	if _, err := pool.Exec(ctx, `
INSERT INTO wallets (id, provider_id, player_id, currency) VALUES ('w3', 'provider-b', 'p1', 'BRL')`); err != nil {
		t.Errorf("expected ok for different provider, got %v", err)
	}

	// (provider_id, external_tx_id) único para transações externas.
	if _, err := pool.Exec(ctx, `
INSERT INTO wagering_transactions (id, provider_id, player_id, wallet_id, round_id, game_id, kind, amount_units, currency, external_tx_id, idempotency_key, payload_hash, occurred_at, state)
VALUES ('x1', 'provider-a', 'p1', 'w1', 'r1', 'g1', 'BET', 100, 'BRL', 'tx-1', 'provider-a:tx-1', 'hash', now(), 'PROCESSED')`); err != nil {
		t.Fatalf("insert wagering tx: %v", err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO wagering_transactions (id, provider_id, player_id, wallet_id, round_id, game_id, kind, amount_units, currency, external_tx_id, idempotency_key, payload_hash, occurred_at, state)
VALUES ('x2', 'provider-a', 'p1', 'w1', 'r1', 'g1', 'BET', 100, 'BRL', 'tx-1', 'other-key', 'hash2', now(), 'PROCESSED')`); err == nil {
		t.Error("expected error on duplicate (provider_id, external_tx_id)")
	}

	// OPENING interno: metadados externos NULL, permitido.
	if _, err := pool.Exec(ctx, `
INSERT INTO wagering_transactions (id, provider_id, player_id, wallet_id, kind, amount_units, currency, occurred_at, state)
VALUES ('x3', NULL, 'p1', 'w3', 'OPENING', 1000, 'BRL', now(), 'PROCESSED')`); err != nil {
		t.Errorf("expected ok for internal OPENING, got %v", err)
	}
}

func tableExists(ctx context.Context, pool *pgxpool.Pool, name string) bool {
	var exists bool
	err := pool.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, "public."+name).Scan(&exists)
	return err == nil && exists
}

func TestSchemaErrorMessagesIntegration(t *testing.T) {
	// sanity: erros apontam para as constraints esperadas (mensagens legíveis)
	pool := integrationPool(t)
	ctx := context.Background()
	_, err := pool.Exec(ctx, `INSERT INTO wallets (id, provider_id, player_id, currency, balance_units)
VALUES ('bad', 'p', 'p', 'BRL', -5)`)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), `wallets_balance_nonnegative`) {
		t.Errorf("unexpected error: %v", err)
	}
}
