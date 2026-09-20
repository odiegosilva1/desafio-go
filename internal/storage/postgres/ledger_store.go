package postgres

import (
	"context"
	"time"

	"desafio-go/internal/domain/ledger"
	"desafio-go/internal/domain/money"
	"desafio-go/internal/storage/port"
)

// ledgerStore implementa port.LedgerStore (append-only).
type ledgerStore struct{}

// NewLedgerStore constrói o repositório do ledger.
func NewLedgerStore() *ledgerStore { return &ledgerStore{} }

func (s *ledgerStore) Append(ctx context.Context, sc port.TxScope, e ledger.Entry) error {
	_, err := fromScope(sc).Exec(ctx, `
INSERT INTO wallet_ledger (id, wallet_id, transaction_id, direction, amount_units, balance_before, balance_after, created_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		e.ID(), e.WalletID(), e.TransactionID(), string(e.Direction()),
		e.Money().Units(), e.BalanceBefore().Units(), e.BalanceAfter().Units(), e.CreatedAt())
	if isUniqueViolation(err) {
		return port.ErrConflict
	}
	return err
}

func (s *ledgerStore) ListByWallet(ctx context.Context, sc port.TxScope, walletID string, afterSeq int64, limit int) ([]ledger.Entry, int64, error) {
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	rows, err := fromScope(sc).Query(ctx, `
SELECT l.id, l.wallet_id, l.transaction_id, l.direction, l.amount_units,
       l.balance_before, l.balance_after, l.created_at, l.seq, w.currency
  FROM wallet_ledger l
  JOIN wallets w ON w.id = l.wallet_id
 WHERE l.wallet_id = $1 AND l.seq > $2
 ORDER BY l.seq
 LIMIT $3`,
		walletID, afterSeq, limit)
	if err != nil {
		return nil, afterSeq, err
	}
	defer rows.Close()

	entries := make([]ledger.Entry, 0, 16)
	var cursor int64 = afterSeq
	for rows.Next() {
		var (
			entryID, walID, txID, direction, cur string
			amount, before, after                int64
			created                              time.Time
			seq                                  int64
		)
		if err := rows.Scan(&entryID, &walID, &txID, &direction, &amount, &before, &after, &created, &seq, &cur); err != nil {
			return nil, afterSeq, err
		}
		currency := money.Currency(cur)
		m, err := money.FromUnits(currency, amount)
		if err != nil {
			return nil, afterSeq, err
		}
		b, err := money.FromUnits(currency, before)
		if err != nil {
			return nil, afterSeq, err
		}
		a, err := money.FromUnits(currency, after)
		if err != nil {
			return nil, afterSeq, err
		}
		entry, err := ledger.Rehydrate(entryID, walID, txID, ledger.Direction(direction), m, b, a, created)
		if err != nil {
			return nil, afterSeq, err
		}
		entries = append(entries, entry)
		cursor = seq
	}
	return entries, cursor, rows.Err()
}

func (s *ledgerStore) BalanceSum(ctx context.Context, sc port.TxScope, walletID string) (int64, error) {
	var sum int64
	err := fromScope(sc).QueryRow(ctx, `
SELECT COALESCE(SUM(CASE WHEN direction = 'CREDIT' THEN amount_units ELSE -amount_units END), 0)
  FROM wallet_ledger
 WHERE wallet_id = $1`, walletID).Scan(&sum)
	return sum, err
}

func (s *ledgerStore) CountByWallet(ctx context.Context, sc port.TxScope, walletID string) (int, error) {
	var count int
	err := fromScope(sc).QueryRow(ctx, `
SELECT COUNT(*) FROM wallet_ledger WHERE wallet_id = $1`, walletID).Scan(&count)
	return count, err
}
