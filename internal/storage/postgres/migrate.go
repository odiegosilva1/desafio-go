package postgres

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	migrationUp   = "up"
	migrationDown = "down"
)

// migration é uma versão com os scripts de aplicação e reversão.
type migration struct {
	version int
	name    string
	up      string
	down    string
}

// Migrator aplica e reverte migrations SQL versionadas contidas em um fs.FS.
//
// Convenção de arquivos: NNNN_nome.up.sql e NNNN_nome.down.sql, onde NNNN é a
// versão decimal ordenável. Cada arquivo contém apenas comandos SQL (sem
// BEGIN/COMMIT): o migrator envolve cada versão em transação própria e a
// registra na tabela schema_version.
type Migrator struct {
	pool *pgxpool.Pool
	fsys fs.FS
}

// NewMigrator constrói um migrator para o pool e o sistema de arquivos dados.
func NewMigrator(pool *pgxpool.Pool, fsys fs.FS) *Migrator {
	return &Migrator{pool: pool, fsys: fsys}
}

// Up aplica todas as migrations pendentes, em ordem crescente de versão.
func (m *Migrator) Up(ctx context.Context) error {
	migrations, err := loadMigrations(m.fsys)
	if err != nil {
		return err
	}
	if err := ensureSchemaVersion(ctx, m.pool); err != nil {
		return err
	}
	applied, err := appliedVersions(ctx, m.pool)
	if err != nil {
		return err
	}
	for _, mig := range migrations {
		if applied[mig.version] {
			continue
		}
		if err := applyMigration(ctx, m.pool, mig, true); err != nil {
			return fmt.Errorf("postgres: migration %04d_%s.up: %w", mig.version, mig.name, err)
		}
	}
	return nil
}

// Down reverte a migration de maior versão já aplicada e retorna a versão
// revertida. Retorna 0 quando não há nada a reverter.
func (m *Migrator) Down(ctx context.Context) (int, error) {
	migrations, err := loadMigrations(m.fsys)
	if err != nil {
		return 0, err
	}
	if err := ensureSchemaVersion(ctx, m.pool); err != nil {
		return 0, err
	}
	applied, err := appliedVersions(ctx, m.pool)
	if err != nil {
		return 0, err
	}
	for i := len(migrations) - 1; i >= 0; i-- {
		mig := migrations[i]
		if !applied[mig.version] {
			continue
		}
		if err := applyMigration(ctx, m.pool, mig, false); err != nil {
			return mig.version, fmt.Errorf("postgres: migration %04d_%s.down: %w", mig.version, mig.name, err)
		}
		return mig.version, nil
	}
	return 0, nil
}

// applyMigration executa o script dentro de uma transação dedicada em modo de
// protocolo simples (permite múltiplos comandos) e registra/remove a versão
// em schema_version na mesma transação.
func applyMigration(ctx context.Context, pool *pgxpool.Pool, mig migration, up bool) error {
	sql := mig.down
	action := "DELETE FROM schema_version WHERE version = $1"
	if up {
		if mig.up == "" {
			return errors.New("migration has no .up.sql")
		}
		sql = mig.up
		action = "INSERT INTO schema_version (version, name) VALUES ($1, $2) ON CONFLICT (version) DO NOTHING"
	}

	conn, err := simpleProtoConn(ctx, pool)
	if err != nil {
		return err
	}
	defer conn.Close(context.WithoutCancel(ctx))

	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	if _, err := tx.Exec(ctx, sql); err != nil {
		return err
	}
	if up {
		if _, err := tx.Exec(ctx, action, mig.version, mig.name); err != nil {
			return err
		}
	} else {
		if _, err := tx.Exec(ctx, action, mig.version); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// simpleProtoConn abre uma conexão dedicada com protocolo simples, necessária
// para executar scripts SQL com múltiplos comandos dentro de uma transação.
func simpleProtoConn(ctx context.Context, pool *pgxpool.Pool) (*pgx.Conn, error) {
	cfg := pool.Config().ConnConfig.Copy()
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	return pgx.ConnectConfig(ctx, cfg)
}

func ensureSchemaVersion(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, `
CREATE TABLE IF NOT EXISTS schema_version (
    version    INT         NOT NULL PRIMARY KEY,
    name       TEXT        NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
)`)
	return err
}

func appliedVersions(ctx context.Context, pool *pgxpool.Pool) (map[int]bool, error) {
	rows, err := pool.Query(ctx, `SELECT version FROM schema_version`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	applied := make(map[int]bool)
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		applied[v] = true
	}
	return applied, rows.Err()
}

// loadMigrations lê, valida e ordena as migrations do fs.FS.
func loadMigrations(fsys fs.FS) ([]migration, error) {
	entries, err := fs.ReadDir(fsys, ".")
	if err != nil {
		return nil, fmt.Errorf("postgres: read migrations dir: %w", err)
	}

	byVersion := make(map[int]*migration)
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		version, dir, name, ok := parseMigrationName(e.Name())
		if !ok {
			continue
		}
		data, err := fs.ReadFile(fsys, e.Name())
		if err != nil {
			return nil, fmt.Errorf("postgres: read migration %s: %w", e.Name(), err)
		}
		mig := byVersion[version]
		if mig == nil {
			mig = &migration{version: version, name: name}
			byVersion[version] = mig
		}
		switch dir {
		case migrationUp:
			mig.up = string(data)
		case migrationDown:
			mig.down = string(data)
		}
	}

	if len(byVersion) == 0 {
		return nil, errors.New("postgres: no migrations found")
	}

	versions := make([]int, 0, len(byVersion))
	for v := range byVersion {
		versions = append(versions, v)
	}
	sort.Ints(versions)

	migrations := make([]migration, 0, len(versions))
	for _, v := range versions {
		mig := *byVersion[v]
		if mig.up == "" {
			return nil, fmt.Errorf("postgres: migration %04d has no .up.sql", v)
		}
		migrations = append(migrations, mig)
	}
	return migrations, nil
}

// parseMigrationName extrai versão, direção e nome de um arquivo NNNN_nome.ext.
// Retorna ok=false para arquivos que não são migrations versionadas.
func parseMigrationName(name string) (version int, dir, mname string, ok bool) {
	if !strings.HasSuffix(name, ".sql") {
		return 0, "", "", false
	}
	base := strings.TrimSuffix(name, ".sql")
	idx := strings.IndexByte(base, '_')
	if idx < 0 {
		return 0, "", "", false
	}
	v, err := strconv.Atoi(base[:idx])
	if err != nil {
		return 0, "", "", false
	}
	rest := base[idx+1:]
	switch {
	case strings.HasSuffix(rest, ".up"):
		n := strings.TrimSuffix(rest, ".up")
		if strings.Contains(n, ".") {
			return 0, "", "", false
		}
		return v, migrationUp, n, true
	case strings.HasSuffix(rest, ".down"):
		n := strings.TrimSuffix(rest, ".down")
		if strings.Contains(n, ".") {
			return 0, "", "", false
		}
		return v, migrationDown, n, true
	default:
		return 0, "", "", false
	}
}
