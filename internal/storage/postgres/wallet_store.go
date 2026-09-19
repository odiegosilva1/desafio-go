package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"desafio-go/internal/domain/money"
	"desafio-go/internal/domain/wallet"
	"desafio-go/internal/storage/port"
)

// walletStore implementa port.WalletStore com locks pessimistas por carteira.
type walletStore struct{}

// NewWalletStore constrói o repositório de carteiras.
func NewWalletStore() *walletStore { return &walletStore{} }

func (s *walletStore) GetByID(ctx context.Context, sc port.TxScope, walletID string) (wallet.Wallet, error) {
	var (
		id, provider, player, cur string
		balanceUnits, version     int64
		created, updated          time.Time
	)
	err := fromScope(sc).QueryRow(ctx, `
SELECT id, provider_id, player_id, currency, balance_units, version, created_at, updated_at
  FROM wallets
 WHERE id = $1`,
		walletID).Scan(&id, &provider, &player, &cur, &balanceUnits, &version, &created, &updated)
	if errors.Is(err, pgx.ErrNoRows) {
		return wallet.Wallet{}, port.ErrNotFound
	}
	if err != nil {
		return wallet.Wallet{}, err
	}
	currency := money.Currency(cur)
	balance, err := money.FromUnits(currency, balanceUnits)
	if err != nil {
		return wallet.Wallet{}, err
	}
	return wallet.Rehydrate(id, provider, player, currency, balance, int(version), created, updated)
}

func (s *walletStore) Create(ctx context.Context, sc port.TxScope, w wallet.Wallet) error {
	_, err := fromScope(sc).Exec(ctx, `
INSERT INTO wallets (id, provider_id, player_id, currency, balance_units, version, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		w.ID(), w.ProviderID(), w.PlayerID(), string(w.Currency()),
		w.Balance().Units(), w.Version(), w.CreatedAt(), w.UpdatedAt())
	if isUniqueViolation(err) {
		return port.ErrConflict
	}
	return err
}

func (s *walletStore) UpdateBalance(ctx context.Context, sc port.TxScope, w wallet.Wallet) error {
	tag, err := fromScope(sc).Exec(ctx, `
UPDATE wallets
   SET balance_units = $3, version = $4, updated_at = $5
 WHERE id = $1 AND provider_id = $2 AND version = $6 AND status = 'ACTIVE'`,
		w.ID(), w.ProviderID(), w.Balance().Units(), w.Version(), w.UpdatedAt(), w.Version()-1)
	if err != nil {
		return err
	}
	// Zero linhas: o lock FOR UPDATE serializou a leitura, mas algo ainda
	// alterou a versão — escrita concorrente detectada, caso de uso faz retry.
	if tag.RowsAffected() != 1 {
		return port.ErrConcurrentUpdate
	}
	return nil
}

func (s *walletStore) Get(ctx context.Context, sc port.TxScope, walletID, providerID string) (wallet.Wallet, error) {
	var (
		id, provider, player, cur string
		balanceUnits, version     int64
		created, updated          time.Time
	)
	err := fromScope(sc).QueryRow(ctx, `
SELECT id, provider_id, player_id, currency, balance_units, version, created_at, updated_at
  FROM wallets
 WHERE id = $1 AND provider_id = $2
   FOR UPDATE`,
		walletID, providerID).Scan(&id, &provider, &player, &cur, &balanceUnits, &version, &created, &updated)
	if errors.Is(err, pgx.ErrNoRows) {
		return wallet.Wallet{}, port.ErrNotFound
	}
	if err != nil {
		return wallet.Wallet{}, err
	}

	currency := money.Currency(cur)
	balance, err := money.FromUnits(currency, balanceUnits)
	if err != nil {
		return wallet.Wallet{}, err
	}
	return wallet.Rehydrate(id, provider, player, currency, balance, int(version), created, updated)
}
