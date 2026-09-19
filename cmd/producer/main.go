package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"

	"desafio-go/internal/config"
	"desafio-go/internal/messaging"
	"desafio-go/internal/observability"
)

const usageText = `producer — enfileira transações de aposta em wager-transactions.fifo

Uso:
  producer send <opções>      enfileira envelopes (objeto ou lista JSON)
  producer wager <opções>     constrói e enfileira um envelope
  producer sample             imprime um envelope de exemplo

send:
  -file PATH   arquivo JSON (objeto ou lista); '-' lê da stdin (default)
  -repeat N    enfileira N cópias idênticas (regenera messageId; exige
               messageId vazio no arquivo)
  -group ID    MessageGroupId (default: data.walletId)
  -dedup ID    MessageDeduplicationId (default: messageId do envelope)
  -queue NAME  fila de entrada (default: $AWS_SQS_QUEUE)

wager:
  -provider ID    (default provider-a)
  -ext-id ID      externalTransactionId (obrigatório)
  -idempotency K  default provider:ext-id
  -wallet ID      (obrigatório)   -player ID (obrigatório)
  -round ID       (obrigatório)   -game ID (obrigatório)
  -kind K         BET|WIN|LOSS|REFUND|ROLLBACK (default BET)
  -amount V       (default 25.00)  -currency C (default BRL)
  -ref EXT        referenceExternalTransactionId (REFUND/ROLLBACK)
  -correlation ID correlationId
  -message-id ID  default gerado   -occurred-at T  RFC3339 (default agora UTC)
  -group ID / -dedup ID / -queue NAME / -repeat N

Ambiente: AWS_ENDPOINT_URL, AWS_REGION, AWS_SQS_QUEUE,
          AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY`

func main() {
	if err := run(os.Args[1:]); err != nil {
		observability.Error(context.Background(), observability.NewLogger("error"), "producer failed", "error", err.Error())
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("producer: subcomando ausente\n%s", usageText)
	}
	ctx := context.Background()
	switch args[0] {
	case "send":
		return runSend(ctx, args[1:])
	case "wager":
		return runWager(ctx, args[1:])
	case "sample":
		_, err := fmt.Fprintln(os.Stdout, sampleEnvelope)
		return err
	case "help", "-h", "--help":
		_, err := fmt.Fprintln(os.Stdout, usageText)
		return err
	default:
		return fmt.Errorf("producer: subcomando desconhecido %q\n%s", args[0], usageText)
	}
}

func runSend(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("send", flag.ContinueOnError)
	var (
		file   = fs.String("file", "-", "arquivo JSON ('-' = stdin)")
		queue  = fs.String("queue", "", "fila de entrada (default: AWS_SQS_QUEUE)")
		group  = fs.String("group", "", "MessageGroupId (default: data.walletId)")
		dedup  = fs.String("dedup", "", "MessageDeduplicationId (default: messageId)")
		repeat = fs.Int("repeat", 1, "cópias idênticas (regenera messageId)")
		level  = fs.String("log-level", "", "nível de log (info|debug|warn|error)")
	)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() > 0 {
		return fmt.Errorf("producer: send não aceita argumentos posicionais: %v", fs.Args())
	}

	var r io.Reader = os.Stdin
	if *file != "-" {
		f, err := os.Open(*file)
		if err != nil {
			return fmt.Errorf("producer: abrir %s: %w", *file, err)
		}
		defer f.Close()
		r = f
	}
	envs, err := readEnvelopes(r)
	if err != nil {
		return err
	}

	awsCfg := config.LoadAWS()
	if *queue != "" {
		awsCfg.InputQueue = *queue
	}
	_, err = connectAndSend(ctx, awsCfg, envs, sendOptions{
		groupOverride: *group,
		dedupOverride: *dedup,
		repeat:        *repeat,
	}, *level)
	return err
}

func runWager(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("wager", flag.ContinueOnError)
	var (
		messageID  = fs.String("message-id", "", "messageId (default: gerado)")
		typ        = fs.String("type", "WagerTransactionRequested", "type do envelope")
		occurredAt = fs.String("occurred-at", "", "RFC3339 (default: agora UTC)")
		provider   = fs.String("provider", "provider-a", "providerId")
		extID      = fs.String("ext-id", "", "externalTransactionId (obrigatório)")
		key        = fs.String("idempotency", "", "default provider:ext-id")
		player     = fs.String("player", "", "playerId (obrigatório)")
		wallet     = fs.String("wallet", "", "walletId (obrigatório)")
		round      = fs.String("round", "", "roundId (obrigatório)")
		game       = fs.String("game", "", "gameId (obrigatório)")
		kind       = fs.String("kind", "BET", "BET|WIN|LOSS|REFUND|ROLLBACK")
		amount     = fs.String("amount", "25.00", "amount")
		currency   = fs.String("currency", "BRL", "currency")
		ref        = fs.String("ref", "", "referenceExternalTransactionId (REFUND/ROLLBACK)")
		correl     = fs.String("correlation", "", "correlationId")
		queue      = fs.String("queue", "", "fila de entrada (default: AWS_SQS_QUEUE)")
		group      = fs.String("group", "", "MessageGroupId (default: data.walletId)")
		dedup      = fs.String("dedup", "", "MessageDeduplicationId (default: messageId)")
		repeat     = fs.Int("repeat", 1, "cópias idênticas (regenera messageId)")
		level      = fs.String("log-level", "", "nível de log (info|debug|warn|error)")
	)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() > 0 {
		return fmt.Errorf("producer: wager não aceita argumentos posicionais: %v", fs.Args())
	}

	env, err := buildWagerEnvelope(wagerOptions{
		messageID:   *messageID,
		typ:         *typ,
		occurredAt:  *occurredAt,
		provider:    *provider,
		extID:       *extID,
		idempotKey:  *key,
		player:      *player,
		wallet:      *wallet,
		round:       *round,
		game:        *game,
		kind:        *kind,
		amount:      *amount,
		currency:    *currency,
		ref:         *ref,
		correlation: *correl,
	})
	if err != nil {
		return err
	}

	awsCfg := config.LoadAWS()
	if *queue != "" {
		awsCfg.InputQueue = *queue
	}
	_, err = connectAndSend(ctx, awsCfg, []messaging.InboundEnvelope{env}, sendOptions{
		groupOverride: *group,
		dedupOverride: *dedup,
		repeat:        *repeat,
	}, *level)
	return err
}
