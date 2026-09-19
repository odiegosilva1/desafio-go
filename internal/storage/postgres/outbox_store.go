package postgres

import (
	"context"
	"time"

	"desafio-go/internal/storage/port"
)

// outboxStore implementa port.OutboxStore com disputa não bloqueante (SKIP LOCKED).
type outboxStore struct{}

// NewOutboxStore constrói o repositório da transactional outbox.
func NewOutboxStore() *outboxStore { return &outboxStore{} }

func (s *outboxStore) Append(ctx context.Context, sc port.TxScope, records ...port.OutboxRecord) error {
	for _, r := range records {
		// ON CONFLICT (id) DO NOTHING preserva o eventId em republicações: um
		// reprocessamento idempotente não duplica nem cloba o registro.
		_, err := fromScope(sc).Exec(ctx, `
INSERT INTO outbox (id, aggregate_type, aggregate_id, event_type, payload,
                    correlation_id, causation_id, occurred_at, version,
                    status, attempts, next_attempt_at)
VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7, $8, $9, 'PENDING', 0, now())
ON CONFLICT (id) DO NOTHING`,
			r.EventID, r.AggregateType, r.AggregateID, r.EventType, string(r.Payload),
			r.CorrelationID, r.CausationID, r.OccurredAt, r.Version)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *outboxStore) Claim(ctx context.Context, sc port.TxScope, batchSize int, now time.Time) ([]port.OutboxRecord, error) {
	rows, err := fromScope(sc).Query(ctx, `
SELECT id, aggregate_type, aggregate_id, event_type, payload,
       correlation_id, causation_id, occurred_at, version
  FROM outbox
 WHERE status = 'PENDING' AND next_attempt_at <= $1
 ORDER BY next_attempt_at, id
 LIMIT $2
   FOR UPDATE SKIP LOCKED`, now, batchSize)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []port.OutboxRecord
	for rows.Next() {
		var (
			id, aggType, aggID, eventType string
			payload                       []byte
			corr, causa                   *string
			occurred                      time.Time
			version                       int
		)
		if err := rows.Scan(&id, &aggType, &aggID, &eventType, &payload, &corr, &causa, &occurred, &version); err != nil {
			return nil, err
		}
		out = append(out, port.OutboxRecord{
			EventID:       id,
			AggregateType: aggType,
			AggregateID:   aggID,
			EventType:     eventType,
			Payload:       payload,
			CorrelationID: strOr(corr),
			CausationID:   strOr(causa),
			OccurredAt:    occurred,
			Version:       version,
		})
	}
	return out, rows.Err()
}

func (s *outboxStore) MarkPublished(ctx context.Context, sc port.TxScope, eventID, publishedBy string, now time.Time) error {
	// 0 linhas = outro publisher já confirmou (republicação concorrente), o que
	// não é erro: o eventId foi preservado.
	_, err := fromScope(sc).Exec(ctx, `
UPDATE outbox
   SET status = 'PUBLISHED', published_at = $2, published_by = $3
  WHERE id = $1 AND status = 'PENDING'`, eventID, now, publishedBy)
	return err
}
