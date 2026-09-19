// Command replay inspeciona e recupera mensagens da DLQ
// (wager-transactions-dlq.fifo) reenviando-as verbatim para a fila de entrada.
//
// Cada reenvio usa MessageDeduplicationId novo (UUID): a janela de deduplicação
// do SQS não engole o replay e reexecuções são inofensivas, pois o consumidor
// deduplica financeiramente por (providerId, idempotencyKey) e pela inbox.
package main

import (
	"context"
	"crypto/sha256"
	"fmt"
	"log/slog"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"

	"desafio-go/internal/messaging"
	"desafio-go/internal/observability"
)

// replayOptions controla scan/requeue.
type replayOptions struct {
	// limit é o teto de mensagens; 0 = até esvaziar (requeue).
	limit int
	// del remove da DLQ após reenvio bem-sucedido (somente requeue).
	del bool
	// groupOverride força MessageGroupId; vazio usa data.walletId do corpo.
	groupOverride string
}

// groupFallback identifica o agrupamento de mensagens ilegíveis (serão
// reenviadas na fila; o consumidor as devolve à DLQ como INVALID_MESSAGE).
const groupFallback = "wager-dlq"

// scan recebe um lote da DLQ e relata o conteúdo sem destruí-lo.
func scan(ctx context.Context, client messaging.SQSClient, dlqURL string, opts replayOptions, logger *slog.Logger) (int, error) {
	out, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:              aws.String(dlqURL),
		MaxNumberOfMessages:   10,
		WaitTimeSeconds:       1,
		VisibilityTimeout:     messaging.VisibilityTimeout,
		MessageAttributeNames: []string{"All"},
	})
	if err != nil {
		return 0, fmt.Errorf("replay: receber da DLQ %s: %w", dlqURL, err)
	}
	for _, msg := range out.Messages {
		body := aws.ToString(msg.Body)
		env, perr := messaging.ParseEnvelope(body)
		observability.Info(ctx, logger, "dlq message",
			"sqsMessageId", aws.ToString(msg.MessageId),
			"failureCode", msg.Attributes["FailureCode"],
			"walletId", envFrom(env, perr).Data.WalletID,
			"transactionId", envFrom(env, perr).Data.ExternalTransactionID,
			"body", body)
	}
	return len(out.Messages), nil
}

// envFrom devolve um envelope vazio quando não legível.
func envFrom(env messaging.InboundEnvelope, err error) messaging.InboundEnvelope {
	if err != nil {
		return messaging.InboundEnvelope{}
	}
	return env
}

// requeue devolve as mensagens da DLQ à fila de entrada verbatim, com
// MessageGroupId = data.walletId e MessageDeduplicationId novo. Com opts.del
// remove da DLQ após o reenvio. Corpos repetidos no mesmo run são enviados uma
// única vez (guarda por hash do corpo).
func requeue(ctx context.Context, client messaging.SQSClient, dlqURL, inputURL string, opts replayOptions, logger *slog.Logger) (requeued, deleted int, err error) {
	seen := map[string]bool{}
	for {
		if opts.limit > 0 && requeued >= opts.limit {
			return requeued, deleted, nil
		}
		out, rerr := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:              aws.String(dlqURL),
			MaxNumberOfMessages:   10,
			WaitTimeSeconds:       1,
			VisibilityTimeout:     60,
			MessageAttributeNames: []string{"All"},
		})
		if rerr != nil {
			return requeued, deleted, fmt.Errorf("replay: receber da DLQ %s: %w", dlqURL, rerr)
		}
		if len(out.Messages) == 0 {
			return requeued, deleted, nil
		}

		for _, msg := range out.Messages {
			if opts.limit > 0 && requeued >= opts.limit {
				return requeued, deleted, nil
			}
			body := aws.ToString(msg.Body)
			hash := fmt.Sprintf("%x", sha256.Sum256([]byte(body)))
			if seen[hash] {
				continue
			}
			seen[hash] = true

			env, perr := messaging.ParseEnvelope(body)
			group := opts.groupOverride
			if group == "" {
				if perr == nil {
					group = env.Data.WalletID
				} else {
					group = groupFallback
				}
			}

			attrs := map[string]types.MessageAttributeValue{
				"ReplayedFrom": {DataType: aws.String("String"), StringValue: aws.String("dlq")},
			}
			if fc := msg.Attributes["FailureCode"]; fc != "" {
				attrs["FailureCode"] = types.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String(fc)}
			}

			if _, serr := client.SendMessage(ctx, &sqs.SendMessageInput{
				QueueUrl:               aws.String(inputURL),
				MessageBody:            aws.String(body),
				MessageGroupId:         aws.String(group),
				MessageDeduplicationId: aws.String(uuid.NewString()),
				MessageAttributes:      attrs,
			}); serr != nil {
				return requeued, deleted, fmt.Errorf("replay: reenviar para %s: %w", inputURL, serr)
			}
			requeued++
			observability.Info(ctx, logger, "dlq message requeued",
				"sqsMessageId", aws.ToString(msg.MessageId),
				"failureCode", msg.Attributes["FailureCode"],
				"walletId", envFrom(env, perr).Data.WalletID,
				"transactionId", envFrom(env, perr).Data.ExternalTransactionID)

			if opts.del {
				if _, derr := client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
					QueueUrl:      aws.String(dlqURL),
					ReceiptHandle: msg.ReceiptHandle,
				}); derr != nil {
					observability.Warn(ctx, logger, "replay: falha ao remover da DLQ",
						"sqsMessageId", aws.ToString(msg.MessageId), "error", derr.Error())
				} else {
					deleted++
				}
			}
		}
	}
}
