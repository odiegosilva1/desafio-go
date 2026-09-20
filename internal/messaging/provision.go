package messaging

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

const (
	// VisibilityTimeout é o prazo de invisibilidade da mensagem em processamento.
	VisibilityTimeout = 30
	// MaxReceiveCount é o número de recebimentos antes do redrive à DLQ.
	MaxReceiveCount = 5
)

// ProvisionQueues cria e configura as filas de entrada e o destino de eventos:
// wager-transactions.fifo (com redrive) e wager-transactions-dlq.fifo, além de
// wallet-events.fifo para os eventos de saída. Operação idempotente.
func ProvisionQueues(ctx context.Context, client SQSClient, region, inputQueue, dlq, eventQueue string) (inputURL, dlqURL, eventURL string, err error) {
	dlqURL, err = ensureFIFOQueue(ctx, client, dlq, nil)
	if err != nil {
		return "", "", "", err
	}

	redrive, err := json.Marshal(map[string]any{
		"deadLetterTargetArn": fifoQueueARN(region, dlq),
		"maxReceiveCount":     MaxReceiveCount,
	})
	if err != nil {
		return "", "", "", err
	}

	inputURL, err = ensureFIFOQueue(ctx, client, inputQueue, map[string]string{
		"RedrivePolicy":          string(redrive),
		"VisibilityTimeout":      fmt.Sprint(VisibilityTimeout),
		"MessageRetentionPeriod": fmt.Sprint(4 * 24 * 3600),
	})
	if err != nil {
		return "", "", "", err
	}

	eventURL, err = ensureFIFOQueue(ctx, client, eventQueue, nil)
	if err != nil {
		return "", "", "", err
	}
	return inputURL, dlqURL, eventURL, nil
}

func ensureFIFOQueue(ctx context.Context, client SQSClient, name string, attributes map[string]string) (string, error) {
	if !queueNameIsFIFO(name) {
		return "", fmt.Errorf("messaging: queue %q must be a FIFO queue (.fifo)", name)
	}
	attrs := map[string]string{
		"FifoQueue":                 "true",
		"ContentBasedDeduplication": "false",
	}
	for k, v := range attributes {
		attrs[k] = v
	}

	if url, err := resolveQueueURL(ctx, client, name); err == nil {
		return url, nil
	}

	out, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName:  aws.String(name),
		Attributes: attrs,
	})
	if err != nil {
		return "", fmt.Errorf("messaging: create queue %s: %w", name, err)
	}
	return *out.QueueUrl, nil
}

func queueNameIsFIFO(name string) bool {
	return len(name) >= 5 && name[len(name)-5:] == ".fifo"
}

// fifoQueueARN monta o ARN de uma fila FIFO. Em LocalStack o ARN segue o mesmo
// formato da AWS; a conta usada pelo LocalStack é 000000000000.
func fifoQueueARN(region, queueName string) string {
	return "arn:aws:sqs:" + region + ":000000000000:" + queueName
}
