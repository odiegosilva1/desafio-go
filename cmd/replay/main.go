package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"desafio-go/internal/config"
	"desafio-go/internal/messaging"
	"desafio-go/internal/observability"
)

const usageText = `replay — inspeciona e recupera mensagens da DLQ wager-transactions-dlq.fifo

Uso:
  replay scan   <opções>   lista mensagens da DLQ (não-destrutivo)
  replay requeue <opções>  reenvia para a fila de entrada; -delete remove da DLQ

Opções:
  -limit N    máximo de mensagens (default 10; 0 = até esvaziar no requeue)
  -delete     (requeue) remove da DLQ após reenvio bem-sucedido
  -group ID   MessageGroupId (default: data.walletId do corpo)
  -queue NAME fila de entrada (default: $AWS_SQS_QUEUE)
  -dlq NAME   DLQ             (default: $AWS_SQS_DLQ)

Cada reenvio usa MessageDeduplicationId novo (UUID); reexecuções são
inofensivas — o consumidor deduplica por (providerId, idempotencyKey).

Ambiente: AWS_ENDPOINT_URL, AWS_REGION, AWS_SQS_QUEUE, AWS_SQS_DLQ,
          AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY`

func main() {
	if err := run(os.Args[1:]); err != nil {
		observability.Error(context.Background(), observability.NewLogger("error"), "replay failed", "error", err.Error())
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("replay: subcomando ausente\n%s", usageText)
	}
	ctx := context.Background()
	switch args[0] {
	case "scan":
		return runScan(ctx, args[1:])
	case "requeue":
		return runRequeue(ctx, args[1:])
	case "help", "-h", "--help":
		_, err := fmt.Fprintln(os.Stdout, usageText)
		return err
	default:
		return fmt.Errorf("replay: subcomando desconhecido %q\n%s", args[0], usageText)
	}
}

// connect resolve as filas (provisiona se ausentes) e devolve o cliente.
func connect(ctx context.Context, awsCfg config.AWSConfig, level string) (messaging.SQSClient, string, string, error) {
	logger := observability.NewLogger(level)
	client, err := messaging.NewSQSClient(ctx, awsCfg, logger)
	if err != nil {
		return nil, "", "", err
	}
	inputURL, err := messaging.ResolveQueueURL(ctx, client, awsCfg.InputQueue)
	if err != nil {
		return nil, "", "", err
	}
	dlqURL, err := messaging.ResolveQueueURL(ctx, client, awsCfg.DLQ)
	if err != nil {
		return nil, "", "", err
	}
	return client, inputURL, dlqURL, nil
}

func runScan(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("scan", flag.ContinueOnError)
	var (
		limit = fs.Int("limit", 10, "máximo de mensagens (até 10 por chamada)")
		queue = fs.String("queue", "", "fila de entrada (default: AWS_SQS_QUEUE)")
		dlq   = fs.String("dlq", "", "DLQ (default: AWS_SQS_DLQ)")
		level = fs.String("log-level", "", "nível de log")
	)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() > 0 {
		return fmt.Errorf("replay: scan não aceita argumentos posicionais")
	}

	awsCfg := config.LoadAWS()
	if *queue != "" {
		awsCfg.InputQueue = *queue
	}
	if *dlq != "" {
		awsCfg.DLQ = *dlq
	}
	client, _, dlqURL, err := connect(ctx, awsCfg, *level)
	if err != nil {
		return err
	}

	logger := observability.NewLogger(*level)
	got, err := scan(ctx, client, dlqURL, replayOptions{limit: *limit}, logger)
	if err != nil {
		return err
	}
	fmt.Printf("mensagens na DLQ: %d\n", got)
	return nil
}

func runRequeue(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("requeue", flag.ContinueOnError)
	var (
		limit = fs.Int("limit", 10, "máximo de mensagens (0 = até esvaziar)")
		del   = fs.Bool("delete", false, "remover da DLQ após reenvio")
		group = fs.String("group", "", "MessageGroupId (default: data.walletId)")
		queue = fs.String("queue", "", "fila de entrada (default: AWS_SQS_QUEUE)")
		dlq   = fs.String("dlq", "", "DLQ (default: AWS_SQS_DLQ)")
		level = fs.String("log-level", "", "nível de log")
	)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() > 0 {
		return fmt.Errorf("replay: requeue não aceita argumentos posicionais")
	}

	awsCfg := config.LoadAWS()
	if *queue != "" {
		awsCfg.InputQueue = *queue
	}
	if *dlq != "" {
		awsCfg.DLQ = *dlq
	}
	client, inputURL, dlqURL, err := connect(ctx, awsCfg, *level)
	if err != nil {
		return err
	}

	logger := observability.NewLogger(*level)
	requeued, deleted, err := requeue(ctx, client, dlqURL, inputURL, replayOptions{
		limit:         *limit,
		del:           *del,
		groupOverride: *group,
	}, logger)
	if err != nil {
		return err
	}
	fmt.Printf("reenviadas %d mensagens para a fila de entrada; removidas %d da DLQ\n", requeued, deleted)
	return nil
}
