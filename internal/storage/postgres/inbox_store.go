package postgres

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"

	"desafio-go/internal/storage/port"
)

// inboxStore implementa port.InboxStore (idempotência durável do consumidor).
type inboxStore struct{}

// NewInboxStore constrói o repositório da inbox.
func NewInboxStore() *inboxStore { return &inboxStore{} }

func (s *inboxStore) InsertNew(ctx context.Context, sc port.TxScope, consumerName, messageID, payloadHash string) (bool, error) {
	// ON CONFLICT DO NOTHING: em redelivery o registro já existe e nenhum erro é
	// lançado — uma violação de unicidade abortaria a transação (25P02),
	// invalidando a dedup dentro do mesmo bloco.
	tag, err := fromScope(sc).Exec(ctx, `
INSERT INTO inbox (consumer_name, message_id, payload_hash, status, received_at)
VALUES ($1, $2, $3, 'RECEIVED', now())
ON CONFLICT (consumer_name, message_id) DO NOTHING`,
		consumerName, messageID, payloadHash)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}

func (s *inboxStore) Lookup(ctx context.Context, sc port.TxScope, consumerName, messageID string) (status, payloadHash string, found bool, err error) {
	var st, hash string
	err = fromScope(sc).QueryRow(ctx, `
SELECT status, payload_hash
  FROM inbox
 WHERE consumer_name = $1 AND message_id = $2`,
		consumerName, messageID).Scan(&st, &hash)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", "", false, nil
	}
	if err != nil {
		return "", "", false, err
	}
	return st, hash, true, nil
}

func (s *inboxStore) Complete(ctx context.Context, sc port.TxScope, consumerName, messageID, status, failureCode string) error {
	_, err := fromScope(sc).Exec(ctx, `
UPDATE inbox
   SET status = $3, processed_at = now(), failure_code = $4
 WHERE consumer_name = $1 AND message_id = $2`,
		consumerName, messageID, status, nilStr(failureCode))
	return err
}
