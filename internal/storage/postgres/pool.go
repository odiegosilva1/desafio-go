// Package postgres contém as implementações concretas de persistência em
// PostgreSQL com pgx: pool, migrations, transações e repositórios financeiros.
//
// A camada de domínio permanece independente deste pacote, dependendo apenas
// das interfaces de internal/storage/port.
package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// NewPool cria um pool pgx a partir da URL de conexão, com timeout de
// conexão e validação de configuração (fail-fast).
func NewPool(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("postgres: invalid database url: %w", err)
	}

	// Prazos explícitos para não travar o startup e o shutdown.
	if cfg.ConnConfig.ConnectTimeout == 0 {
		cfg.ConnConfig.ConnectTimeout = 10 * time.Second
	}
	cfg.MaxConnLifetime = 30 * time.Minute
	cfg.MaxConnIdleTime = 5 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("postgres: init pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres: ping failed: %w", err)
	}
	return pool, nil
}
