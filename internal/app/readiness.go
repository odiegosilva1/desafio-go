package app

import (
	"context"
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/jackc/pgx/v5/pgxpool"

	"desafio-go/internal/httpapi"
	"desafio-go/internal/messaging"
)

// newReadiness monta o readiness check do serviço: PostgreSQL e SQS (specs §9:
// "Liveness do processo e readiness de PostgreSQL e SQS").
func newReadiness(pool *pgxpool.Pool, client messaging.SQSClient, queues messaging.Queues) httpapi.Pinger {
	return readinessGroup{
		checks: []httpapi.Pinger{
			pgReadiness{pool: pool},
			sqsReadiness{client: client, queueURL: queues.InputQueueURL},
		},
	}
}

// readinessGroup responde apenas quando todas as dependências respondem.
type readinessGroup struct {
	checks []httpapi.Pinger
}

func (r readinessGroup) Ping(ctx context.Context) error {
	for _, c := range r.checks {
		if err := c.Ping(ctx); err != nil {
			return err
		}
	}
	return nil
}

// pgReadiness verifica a conexão com o PostgreSQL.
type pgReadiness struct{ pool *pgxpool.Pool }

func (p pgReadiness) Ping(ctx context.Context) error {
	return p.pool.Ping(ctx)
}

// sqsReadiness verifica que o endpoint SQS responde e a fila de entrada existe.
type sqsReadiness struct {
	client   messaging.SQSClient
	queueURL string
}

func (s sqsReadiness) Ping(ctx context.Context) error {
	_, err := s.client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{
		QueueName: aws.String(queueNameFromURL(s.queueURL)),
	})
	return err
}

// queueNameFromURL extrai o nome da fila da URL (último segmento do path).
func queueNameFromURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	segments := strings.Split(strings.Trim(u.Path, "/"), "/")
	if len(segments) == 0 {
		return raw
	}
	return segments[len(segments)-1]
}
