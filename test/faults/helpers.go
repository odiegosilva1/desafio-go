//go:build integration

package faults

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/jackc/pgx/v5/pgxpool"

	"desafio-go/internal/app"
	"desafio-go/internal/application"
	"desafio-go/internal/domain/money"
	"desafio-go/internal/messaging"
	"desafio-go/internal/observability"
	"desafio-go/internal/storage/postgres"
)

// fakeSQS emula o broker SQS suficiente para dirigir o Consumer e o
// OutboxPublisher: a fila de entrada reentrega a mensagem até o DeleteMessage
// (semântica de visibility), e as saídas (DLQ/eventos) são gravadas em
// memória para inspeção.
type fakeSQS struct {
	mu         sync.Mutex
	input      []types.Message
	receives   int
	dlqSends   []*sqs.SendMessageInput
	eventSends []*sqs.SendMessageInput
	deletes    []*sqs.DeleteMessageInput
	inputURL   string
	dlqURL     string
	eventURL   string
	receiveErr error
	sendErr    error
	deleteErr  error
	// visibilityChanges grava as visibilidades estendidas em falhas transitórias
	// (backoff do consumidor), para asserção nos testes.
	visibilityChanges []int32
}

func newFakeSQS(inputURL, dlqURL, eventURL string) *fakeSQS {
	return &fakeSQS{inputURL: inputURL, dlqURL: dlqURL, eventURL: eventURL}
}

func awsStr(s string) *string { return aws.String(s) }

func (f *fakeSQS) ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.receiveErr != nil {
		return nil, f.receiveErr
	}
	if len(f.input) == 0 {
		return &sqs.ReceiveMessageOutput{}, nil
	}
	head := f.input[0]
	f.input = append(f.input[1:], head)
	f.receives++
	return &sqs.ReceiveMessageOutput{Messages: []types.Message{head}}, nil
}

func (f *fakeSQS) SendMessage(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.sendErr != nil {
		return nil, f.sendErr
	}
	url := aws.ToString(params.QueueUrl)
	out := &sqs.SendMessageOutput{MessageId: awsStr(fmt.Sprintf("fake-%d", len(f.dlqSends)+len(f.eventSends)))}
	switch url {
	case f.dlqURL:
		f.dlqSends = append(f.dlqSends, params)
	case f.eventURL:
		f.eventSends = append(f.eventSends, params)
	}
	return out, nil
}

func (f *fakeSQS) DeleteMessage(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.deleteErr != nil {
		return nil, f.deleteErr
	}
	f.deletes = append(f.deletes, params)
	handle := aws.ToString(params.ReceiptHandle)
	kept := f.input[:0]
	for _, m := range f.input {
		if aws.ToString(m.ReceiptHandle) != handle {
			kept = append(kept, m)
		}
	}
	f.input = kept
	return &sqs.DeleteMessageOutput{}, nil
}

func (f *fakeSQS) ChangeMessageVisibility(ctx context.Context, params *sqs.ChangeMessageVisibilityInput, optFns ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.visibilityChanges = append(f.visibilityChanges, params.VisibilityTimeout)
	return &sqs.ChangeMessageVisibilityOutput{}, nil
}

func (f *fakeSQS) GetQueueUrl(ctx context.Context, params *sqs.GetQueueUrlInput, optFns ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error) {
	return &sqs.GetQueueUrlOutput{QueueUrl: awsStr(f.inputURL)}, nil
}

func (f *fakeSQS) CreateQueue(ctx context.Context, params *sqs.CreateQueueInput, optFns ...func(*sqs.Options)) (*sqs.CreateQueueOutput, error) {
	return &sqs.CreateQueueOutput{QueueUrl: awsStr(f.inputURL)}, nil
}

func (f *fakeSQS) deleteCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.deletes)
}

func (f *fakeSQS) dlqCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.dlqSends)
}

func (f *fakeSQS) eventBodies() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, 0, len(f.eventSends))
	for _, s := range f.eventSends {
		out = append(out, aws.ToString(s.MessageBody))
	}
	return out
}

func (f *fakeSQS) dlqBodies() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, 0, len(f.dlqSends))
	for _, s := range f.dlqSends {
		out = append(out, aws.ToString(s.MessageBody))
	}
	return out
}

func (f *fakeSQS) dlqFailureCodes() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, 0, len(f.dlqSends))
	for _, s := range f.dlqSends {
		out = append(out, aws.ToString(s.MessageAttributes["FailureCode"].StringValue))
	}
	return out
}

func (f *fakeSQS) enqueue(m types.Message) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.input = append(f.input, m)
}

func (f *fakeSQS) receivesCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.receives
}

var _ messaging.SQSClient = (*fakeSQS)(nil)

// dbURL devolve o Postgres de teste (padrão ou DATABASE_URL).
func dbURL() string {
	if url := os.Getenv("DATABASE_URL"); url != "" {
		return url
	}
	return "postgres://wallet:wallet@localhost:5432/wallet?sslmode=disable"
}

func newPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool, err := postgres.NewPool(context.Background(), dbURL())
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// resetSchema zera o banco aplicando Down até a versão 0 e Up novamente.
func resetSchema(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	files, err := app.Migrations()
	if err != nil {
		t.Fatalf("migrations: %v", err)
	}
	mig := postgres.NewMigrator(pool, files)
	for {
		v, err := mig.Down(ctx)
		if err != nil {
			t.Fatalf("down cleanup: %v", err)
		}
		if v == 0 {
			break
		}
	}
	if err := mig.Up(ctx); err != nil {
		t.Fatalf("up: %v", err)
	}
}

func newRepos(pool *pgxpool.Pool) application.Repos {
	return application.Repos{
		Wallets:  postgres.NewWalletStore(),
		Ledger:   postgres.NewLedgerStore(),
		Wagering: postgres.NewWageringStore(),
		Outbox:   postgres.NewOutboxStore(),
		Inbox:    postgres.NewInboxStore(),
		UOW:      postgres.NewUnitOfWork(pool),
	}
}

func newService(t *testing.T, pool *pgxpool.Pool) *application.Service {
	t.Helper()
	return application.NewService(newRepos(pool), observability.NewLogger("error"), observability.NewMetrics())
}

// newConsumer monta um Consumer real do pacote de mensageria sobre o pool dado,
// dirigindo o fakeSQS como cliente do broker.
func newConsumer(t *testing.T, f *fakeSQS, pool *pgxpool.Pool) *messaging.Consumer {
	t.Helper()
	svc := newService(t, pool)
	repos := newRepos(pool)
	return messaging.NewConsumer(f, f.inputURL, f.dlqURL, svc, repos, observability.NewLogger("error"), observability.NewMetrics(), 20*time.Millisecond)
}

// runConsumer roda o Consumer em background e retorna um sinal de término.
func runConsumer(ctx context.Context, c *messaging.Consumer) chan struct{} {
	done := make(chan struct{})
	go func() {
		_ = c.Run(ctx)
		close(done)
	}()
	return done
}

// message monta uma mensagem SQS de entrada com o ID e o corpo dados.
func message(id, body string) types.Message {
	return types.Message{
		MessageId:     awsStr(id),
		ReceiptHandle: awsStr("rh-" + id),
		Body:          awsStr(body),
	}
}

// betEnvelope serializa um envelope BET no wire canônico do consumidor.
func betEnvelope(messageID, walletID, externalTxID, amount string) (string, error) {
	bin, err := messaging.MarshalInbound(messaging.InboundEnvelope{
		MessageID:  messageID,
		Type:       "WAGER",
		OccurredAt: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
		Data: messaging.InboundWagerData{
			ProviderID:            "p1",
			ExternalTransactionID: externalTxID,
			IdempotencyKey:        externalTxID,
			PlayerID:              "player-1",
			WalletID:              walletID,
			RoundID:               "r1",
			GameID:                "g1",
			Kind:                  "BET",
			Money:                 messaging.MoneyJSON{Amount: amount, Currency: "BRL"},
		},
	})
	if err != nil {
		return "", err
	}
	return string(bin), nil
}

// openWallet cria uma carteira diretamente via Service (setup determinístico).
func openWallet(t *testing.T, svc *application.Service, playerID string, initial string) string {
	t.Helper()
	bal, err := money.FromDecimalString(initial, money.CurrencyBRL)
	if err != nil {
		t.Fatalf("parse initial balance: %v", err)
	}
	res, err := svc.OpenWallet(context.Background(), application.OpenWalletInput{
		ProviderID:     "p1",
		PlayerID:       playerID,
		InitialBalance: bal,
		OccurredAt:     time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("open wallet: %v", err)
	}
	return res.ID
}

type inboxRow struct {
	Status      string
	FailureCode string
}

func getInbox(t *testing.T, pool *pgxpool.Pool, messageID string) (inboxRow, bool) {
	t.Helper()
	var row inboxRow
	err := pool.QueryRow(context.Background(),
		`SELECT status, coalesce(failure_code,'') FROM inbox WHERE message_id=$1`, messageID,
	).Scan(&row.Status, &row.FailureCode)
	if err != nil {
		return inboxRow{}, false
	}
	return row, true
}

// ledgerWagerCount conta lançamentos de operações de aposta (exclui o OPENING
// que credita a carteira no setup), para isolar o efeito financeiro tratado.
func ledgerWagerCount(t *testing.T, pool *pgxpool.Pool, walletID string) int {
	t.Helper()
	var n int
	err := pool.QueryRow(context.Background(),
		`SELECT count(*)
		   FROM wallet_ledger l
		   JOIN wagering_transactions t ON t.id = l.transaction_id
		  WHERE l.wallet_id = $1 AND t.kind <> 'OPENING'`, walletID).Scan(&n)
	if err != nil {
		t.Fatalf("ledger wager count: %v", err)
	}
	return n
}

func balance(t *testing.T, pool *pgxpool.Pool, walletID string) string {
	t.Helper()
	var units int64
	if err := pool.QueryRow(context.Background(),
		`SELECT balance_units FROM wallets WHERE id=$1`, walletID).Scan(&units); err != nil {
		t.Fatalf("balance: %v", err)
	}
	return formatUnits(units)
}

func formatUnits(units int64) string {
	return fmt.Sprintf("%d.%02d", units/100, units%100)
}

func pendingOutbox(t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(),
		`SELECT count(*) FROM outbox WHERE status='PENDING'`).Scan(&n); err != nil {
		t.Fatalf("pending outbox: %v", err)
	}
	return n
}

func publishedOutbox(t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(),
		`SELECT count(*) FROM outbox WHERE status='PUBLISHED'`).Scan(&n); err != nil {
		t.Fatalf("published outbox: %v", err)
	}
	return n
}

// waitFor aguarda uma condição até o timeout, falhando o teste se não atingir.
func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("timeout aguardando condição")
}

// mustMoney converte uma string decimal em money.Money, falhando o teste.
func mustMoney(t *testing.T, amount string) money.Money {
	t.Helper()
	m, err := money.FromDecimalString(amount, money.CurrencyBRL)
	if err != nil {
		t.Fatalf("parse money %q: %v", amount, err)
	}
	return m
}
