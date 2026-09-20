// Package messaging implementa o consumidor SQS (com inbox durável), o
// publisher da transactional outbox e o provisionamento das filas no
// LocalStack/AWS. O acesso ao broker é controlado por credenciais; as
// validações de domínio permanecem no consumidor (internal/application).
package messaging

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"desafio-go/internal/config"
)

// SQSClient abstrai o subconjunto do cliente AWS SQS usado pela aplicação.
type SQSClient interface {
	GetQueueUrl(ctx context.Context, params *sqs.GetQueueUrlInput, optFns ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error)
	CreateQueue(ctx context.Context, params *sqs.CreateQueueInput, optFns ...func(*sqs.Options)) (*sqs.CreateQueueOutput, error)
	ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessage(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
	SendMessage(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
	// ChangeMessageVisibility estende a visibilidade de uma mensagem em falha
	// transitória (retry com backoff exponencial do consumidor). O `receiptHandle`
	// continua válido para DeleteMessage.
	ChangeMessageVisibility(ctx context.Context, params *sqs.ChangeMessageVisibilityInput, optFns ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error)
}

// NewSQSClient constrói o cliente SQS apontando para o endpoint configurado
// (LocalStack quando AWS_ENDPOINT_URL presente) e o região informada.
func NewSQSClient(ctx context.Context, cfg config.AWSConfig, logger *slog.Logger) (SQSClient, error) {
	optFns := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(cfg.Region),
	}
	if strings.TrimSpace(cfg.EndpointURL) != "" {
		optFns = append(optFns, awsconfig.WithBaseEndpoint(cfg.EndpointURL))
	}
	if cfg.AccessKey != "" {
		optFns = append(optFns, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, "")))
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, optFns...)
	if err != nil {
		return nil, fmt.Errorf("messaging: load aws config: %w", err)
	}
	return sqs.NewFromConfig(awsCfg), nil
}

// Queues armazena as URLs resolvidas das filas após o provisionamento.
type Queues struct {
	InputQueueURL string
	DLQURL        string
	EventQueueURL string
}

// ResolveQueues provisiona (idempotente) as filas e devolve suas URLs. Chamado
// na inicialização da aplicação; em LocalStack as filas são criadas sob demanda.
func ResolveQueues(ctx context.Context, client SQSClient, cfg config.AWSConfig, logger *slog.Logger) (Queues, error) {
	inputURL, dlqURL, eventURL, err := ProvisionQueues(ctx, client, cfg.Region, cfg.InputQueue, cfg.DLQ, cfg.EventQueue)
	if err != nil {
		return Queues{}, err
	}
	return Queues{InputQueueURL: inputURL, DLQURL: dlqURL, EventQueueURL: eventURL}, nil
}

// resolveQueueURL devolve a URL da fila pelo nome.
func resolveQueueURL(ctx context.Context, client SQSClient, queueName string) (string, error) {
	out, err := client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String(queueName)})
	if err != nil {
		return "", fmt.Errorf("messaging: resolve queue %s: %w", queueName, err)
	}
	return *out.QueueUrl, nil
}

// ResolveQueueURL resolve uma única fila FIFO pelo nome, criando-a se
// necessário (operação idempotente). Usada pelas ferramentas de operação que
// não precisam provisionar o pipeline completo.
func ResolveQueueURL(ctx context.Context, client SQSClient, queueName string) (string, error) {
	return ensureFIFOQueue(ctx, client, queueName, nil)
}
