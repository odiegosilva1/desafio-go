package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"desafio-go/internal/domain/money"
	"desafio-go/internal/domain/wagering"
	"desafio-go/internal/storage/port"
)

// wageringStore implementa port.WageringStore.
type wageringStore struct{}

// NewWageringStore constrói o repositório de transações de aposta.
func NewWageringStore() *wageringStore { return &wageringStore{} }

const wageringColumns = `
id, provider_id, player_id, wallet_id, round_id, game_id, kind, amount_units,
currency, external_tx_id, reference_ext_id, reference_internal_id,
idempotency_key, correlation_id, causation_id, occurred_at, recorded_at,
processed_at, wallet_version, failure_code, failure_message, payload_hash,
state, attempts, next_attempt_at, result_balance_units`

func (s *wageringStore) Insert(ctx context.Context, sc port.TxScope, t wagering.Transaction) error {
	_, err := fromScope(sc).Exec(ctx, `
INSERT INTO wagering_transactions (`+wageringColumns+`)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26)`,
		t.ID(),
		nilStr(t.ProviderID()), t.PlayerID(), t.WalletID(),
		nilStr(t.RoundID()), nilStr(t.GameID()), string(t.Kind()),
		t.Money().Units(), string(t.Money().Currency()),
		nilStr(t.ExternalID()), nilStr(t.ReferenceExtID()), nilStr(t.ReferenceInternalID()),
		nilStr(t.IdempotencyKey()), nilStr(t.CorrelationID()), nilStr(t.CausationID()),
		t.OccurredAt(), t.RecordedAt(),
		nilTime(t.ProcessedAt()), t.WalletVersion(),
		nilStr(t.FailureCode()), nilStr(t.FailureMessage()), nilStr(t.PayloadHash()),
		string(t.State()), t.Attempts(), nilTime(t.NextAttemptAt()),
		resultBalanceUnits(t))
	if isUniqueViolation(err) {
		return port.ErrConflict
	}
	return err
}

func (s *wageringStore) Update(ctx context.Context, sc port.TxScope, t wagering.Transaction) error {
	tag, err := fromScope(sc).Exec(ctx, `
UPDATE wagering_transactions
   SET state = $2, processed_at = $3, wallet_version = $4,
       failure_code = $5, failure_message = $6,
       reference_ext_id = $7, reference_internal_id = $8,
       attempts = $9, next_attempt_at = $10, result_balance_units = $11
 WHERE id = $1`,
		t.ID(),
		string(t.State()), nilTime(t.ProcessedAt()), t.WalletVersion(),
		nilStr(t.FailureCode()), nilStr(t.FailureMessage()),
		nilStr(t.ReferenceExtID()), nilStr(t.ReferenceInternalID()),
		t.Attempts(), nilTime(t.NextAttemptAt()), resultBalanceUnits(t))
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return port.ErrNotFound
	}
	return nil
}

func (s *wageringStore) GetByID(ctx context.Context, sc port.TxScope, id string) (wagering.Transaction, error) {
	return scanTransaction(fromScope(sc).QueryRow(ctx,
		`SELECT `+wageringColumns+` FROM wagering_transactions WHERE id = $1`, id))
}

func (s *wageringStore) GetByIdempotencyKey(ctx context.Context, sc port.TxScope, providerID, key string) (wagering.Transaction, error) {
	return scanTransaction(fromScope(sc).QueryRow(ctx,
		`SELECT `+wageringColumns+` FROM wagering_transactions
		  WHERE provider_id = $1 AND idempotency_key = $2`, providerID, key))
}

func (s *wageringStore) GetByExternalTxID(ctx context.Context, sc port.TxScope, providerID, externalTxID string) (wagering.Transaction, error) {
	return scanTransaction(fromScope(sc).QueryRow(ctx,
		`SELECT `+wageringColumns+` FROM wagering_transactions
		  WHERE provider_id = $1 AND external_tx_id = $2`, providerID, externalTxID))
}

func (s *wageringStore) ListReversalsByReference(ctx context.Context, sc port.TxScope, referenceExternalID string) ([]wagering.Transaction, error) {
	rows, err := fromScope(sc).Query(ctx, `
SELECT `+wageringColumns+` FROM wagering_transactions
 WHERE reference_ext_id = $1 AND kind IN ('REFUND', 'ROLLBACK')
 ORDER BY recorded_at, id`, referenceExternalID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []wagering.Transaction
	for rows.Next() {
		t, err := scanTransaction(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

func (s *wageringStore) ListPendingReference(ctx context.Context, sc port.TxScope, now time.Time) ([]wagering.Transaction, error) {
	rows, err := fromScope(sc).Query(ctx, `
SELECT `+wageringColumns+` FROM wagering_transactions
 WHERE state = 'PENDING_REFERENCE' AND (next_attempt_at IS NULL OR next_attempt_at <= $1)
 ORDER BY next_attempt_at, id
 FOR UPDATE SKIP LOCKED`, now)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []wagering.Transaction
	for rows.Next() {
		t, err := scanTransaction(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

func nilTime(t *time.Time) any {
	if t == nil {
		return nil
	}
	return *t
}

// resultBalanceUnits persiste o saldo observado apenas para conclusões
// bem-sucedidas (NULL para estados não processados).
func resultBalanceUnits(t wagering.Transaction) any {
	if t.State() != wagering.StateProcessed {
		return nil
	}
	return t.ResultBalance().Units()
}

// scanner cobre pgx.Row e pgx.Rows.
type scanner interface {
	Scan(dest ...any) error
}

func scanTransaction(row scanner) (wagering.Transaction, error) {
	var (
		id, player, wallet, kindT, cur                      string
		provider, round, game, ext, refExt, refInt          *string
		idemKey, corr, causa, failureCode, failureMsg, hash *string
		amount, walletVersion                               int64
		resultBal                                           *int64
		occurred, recorded                                  time.Time
		processed, nextAttempt                              *time.Time
		state                                               string
		attempts                                            int
	)
	err := row.Scan(
		&id, &provider, &player, &wallet, &round, &game, &kindT, &amount, &cur,
		&ext, &refExt, &refInt, &idemKey, &corr, &causa,
		&occurred, &recorded, &processed, &walletVersion,
		&failureCode, &failureMsg, &hash, &state, &attempts, &nextAttempt, &resultBal,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return wagering.Transaction{}, port.ErrNotFound
	}
	if err != nil {
		return wagering.Transaction{}, err
	}

	currency := money.Currency(cur)
	m, err := money.FromUnits(currency, amount)
	if err != nil {
		return wagering.Transaction{}, err
	}
	result := money.Money{}
	if resultBal != nil {
		result, err = money.FromUnits(currency, *resultBal)
		if err != nil {
			return wagering.Transaction{}, err
		}
	}

	return wagering.Rehydrate(wagering.RehydrateOptions{
		ID:                  id,
		ExternalTxID:        strOr(ext),
		ProviderID:          strOr(provider),
		PlayerID:            player,
		WalletID:            wallet,
		RoundID:             strOr(round),
		GameID:              strOr(game),
		Kind:                wagering.Kind(kindT),
		Money:               m,
		ReferenceExtID:      strOr(refExt),
		ReferenceInternalID: strOr(refInt),
		IdempotencyKey:      strOr(idemKey),
		CorrelationID:       strOr(corr),
		CausationID:         strOr(causa),
		OccurredAt:          occurred,
		RecordedAt:          recorded,
		ProcessedAt:         processed,
		FailureCode:         strOr(failureCode),
		FailureMessage:      strOr(failureMsg),
		WalletVersion:       int(walletVersion),
		PayloadHash:         strOr(hash),
		State:               wagering.State(state),
		Attempts:            attempts,
		NextAttemptAt:       nextAttempt,
		ResultBalance:       result,
	})
}

func strOr(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}
