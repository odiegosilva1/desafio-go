//go:build integration

// Package realstack exercita a malha completa com infraestrutura REAL: a
// aplicação de produção (app.New) é iniciada contra PostgreSQL, LocalStack
// (SQS) e Keycloak (OIDC) de verdade — sem fakeSQS nem fakeVerifier. Cobre
// autenticação efetiva, HTTP, consumidor SQS com inbox, transactional outbox
// publicando no destino e reconciliação. A infra é provida por
// `make test-realstack` (docker compose: postgres, localstack, keycloak).
package realstack

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/jackc/pgx/v5/pgxpool"

	"desafio-go/internal/app"
	"desafio-go/internal/config"
	"desafio-go/internal/domain/event"
	"desafio-go/internal/messaging"
)

const (
	keycloakBase = "http://localhost:8081/realms/wallet"
	sqsEndpoint  = "http://localhost:4566"
	tokenTTL     = 120 * time.Second
)

// TestRealStackEndToEnd roda o fluxo completo contra a malha real (compose):
// Imports do realm Keycloak provisionado, autenticação client_credentials,
// abertura de carteira, aposta via HTTP, aposta via SQS (consumidor real da
// fila), eventos publicados pela outbox no destino e reconciliação.
func TestRealStackEndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	if !postgresReady(ctx) {
		t.Skip("postgres indisponível; rode `make test-realstack` para subir a infra")
	}
	client, queues := realClients(t, ctx)
	if !keycloakReady(ctx) {
		t.Skip("keycloak indisponível; rode `make test-realstack` para subir a infra")
	}

	httpAddr := freeAddr(t)
	t.Setenv("HTTP_ADDR", httpAddr)
	t.Setenv("OIDC_ISSUER_URL", keycloakBase)
	t.Setenv("WORKER_REFERENCE_INTERVAL", "100ms")
	t.Setenv("WORKER_OUTBOX_INTERVAL", "100ms")
	t.Setenv("WORKER_OUTBOX_BATCH", "32")
	t.Setenv("WORKER_SQS_POLL_INTERVAL", "100ms")
	t.Setenv("SHUTDOWN_TIMEOUT", "5s")

	appx := app.New()
	if err := appx.Start(ctx); err != nil {
		t.Fatalf("start real: %v", err)
	}
	defer func() { _ = appx.Stop(context.Background()) }()

	// Aplicação de prontidão: liveness + readiness (Postgres E SQS reais).
	waitFor(t, ctx, 5*time.Second, 200*time.Millisecond, func() bool {
		status, _ := httpGet(ctx, "http://"+httpAddr+"/health/live")
		return status == http.StatusOK
	})
	waitFor(t, ctx, 10*time.Second, 300*time.Millisecond, func() bool {
		status, _ := httpGet(ctx, "http://"+httpAddr+"/health/ready")
		return status == http.StatusOK
	})

	// Autenticação real: token OIDC client_credentials do realm Keycloak.
	tokenA, err := oidcToken(ctx, "provider-a", "provider-a-secret")
	if err != nil {
		t.Fatalf("token provider-a: %v", err)
	}
	tokenInternal, err := oidcToken(ctx, "wallet-service-internal", "wallet-internal-secret")
	if err != nil {
		t.Fatalf("token internal: %v", err)
	}

	stamp := time.Now().UnixNano()
	player := fmt.Sprintf("player-realstack-%d", stamp)

	// Abertura de carteira via HTTP autenticado.
	walletID := openWalletHTTP(t, ctx, httpAddr, tokenA, player)

	// Aposta 1 via HTTP: saldo 100.00 → 75.00.
	processHTTP(t, ctx, httpAddr, tokenA, walletID, player,
		"tx-realstack-http-"+itoa(stamp), "round-1", "BET", "25.00", http.StatusOK)
	assertBalanceHTTP(t, ctx, httpAddr, tokenA, walletID, "75.00")

	// Aposta 2 via SQS REAL: colocada na fila de entrada, consumida pela
	// aplicação (inbox durável + idempotência), saldo 75.00 → 45.00.
	sqsTxID := "tx-realstack-sqs-" + itoa(stamp)
	enqueueBet(t, ctx, client, queues.InputQueueURL, walletID, player,
		sqsTxID, "msg-realstack-"+itoa(stamp), "round-2", "30.00")
	waitFor(t, ctx, 15*time.Second, 300*time.Millisecond, func() bool {
		status, body := httpGetJSON(ctx, "http://"+httpAddr+"/providers/provider-a/wagering/transactions/"+sqsTxID, tokenA)
		if status != http.StatusOK {
			return false
		}
		var res struct {
			Status string `json:"status"`
		}
		_ = json.Unmarshal(body, &res)
		return res.Status == "PROCESSED"
	})
	assertBalanceHTTP(t, ctx, httpAddr, tokenA, walletID, "45.00")

	// Outbox REAL: evento publicado no destino wallet-events.fifo pela
	// transactional outbox (não há degradação para mocks).
	waitFor(t, ctx, 15*time.Second, 500*time.Millisecond, func() bool {
		events, err := receiveEvents(ctx, client, queues.EventQueueURL, 10)
		if err != nil {
			return false
		}
		for _, env := range events {
			if env.EventType != event.TypeWagerTransactionProcessed {
				continue
			}
			data, _ := env.Data.(map[string]any)
			ext, ok := data["externalTransactionId"].(string)
			if ok && ext == sqsTxID {
				return true
			}
		}
		return false
	})

	// Reconciliação → consistente, usando o cliente interno autenticado.
	status, body := httpPostJSON(ctx, "http://"+httpAddr+"/wallets/"+walletID+"/reconciliation", tokenInternal, "{}")
	if status != http.StatusOK {
		t.Fatalf("reconciliation = %d: %s", status, body)
	}
	var rec struct {
		WalletID          string    `json:"walletId"`
		StoredBalance     moneyBody `json:"storedBalance"`
		CalculatedBalance moneyBody `json:"calculatedBalance"`
	}
	if err := json.Unmarshal(body, &rec); err != nil {
		t.Fatalf("reconciliation body: %v", err)
	}
	if rec.WalletID != walletID || rec.StoredBalance.Amount != rec.CalculatedBalance.Amount || rec.CalculatedBalance.Amount != "45.00" {
		t.Fatalf("reconciliation inconsistente: %+v", rec)
	}

	t.Logf("malha real OK: carteira %s, saldo %s, tx SQS %s", walletID, rec.CalculatedBalance.Amount, sqsTxID)
}

// ---------------------------------------------------------------------------
// Infra — prontidão e esperas
// ---------------------------------------------------------------------------

func postgresReady(ctx context.Context) bool {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		databaseURL = "postgres://wallet:wallet@localhost:5432/wallet?sslmode=disable"
	}
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return false
	}
	defer pool.Close()
	return pool.Ping(ctx) == nil
}

// realClients devolve o cliente SQS real (LocalStack) e as URLs das filas
// provisionadas de forma idempotente — o mesmo caminho de `make provision`.
func realClients(t *testing.T, ctx context.Context) (messaging.SQSClient, messaging.Queues) {
	t.Helper()
	t.Setenv("AWS_ENDPOINT_URL", sqsEndpoint)
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	client, err := messaging.NewSQSClient(ctx, config.LoadAWS(), nil)
	if err != nil {
		t.Skipf("SQS/LocalStack indisponível: %v", err)
	}
	inputURL, dlqURL, eventURL, err := messaging.ProvisionQueues(
		ctx, client, "us-east-1",
		"wager-transactions.fifo", "wager-transactions-dlq.fifo", "wallet-events.fifo")
	if err != nil {
		t.Skipf("SQS/LocalStack não provisionou: %v", err)
	}
	return client, messaging.Queues{InputQueueURL: inputURL, DLQURL: dlqURL, EventQueueURL: eventURL}
}

// keycloakReady espera o realm importado responder via token (o contêiner
// importa realm-export.json no primeiro start, que demora alguns segundos).
func keycloakReady(ctx context.Context) bool {
	probe, err := net.DialTimeout("tcp", "localhost:8081", 2*time.Second)
	if err != nil {
		return false
	}
	_ = probe.Close()
	for {
		_, err := oidcToken(ctx, "provider-a", "provider-a-secret")
		if err == nil {
			return true
		}
		if ctx.Err() != nil {
			return false
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func oidcToken(ctx context.Context, clientID, clientSecret string) (string, error) {
	form := url.Values{
		"grant_type":    {"client_credentials"},
		"client_id":     {clientID},
		"client_secret": {clientSecret},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		keycloakBase+"/protocol/openid-connect/token", strings.NewReader(form.Encode()))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("oidc token %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var out struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return "", err
	}
	if out.AccessToken == "" {
		return "", fmt.Errorf("oidc: access_token vazio")
	}
	return out.AccessToken, nil
}

func waitFor(t *testing.T, ctx context.Context, timeout, interval time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			t.Fatalf("waitFor: contexto cancelado: %v", ctx.Err())
		}
		if fn() {
			return
		}
		time.Sleep(interval)
	}
	t.Fatalf("waitFor: condição não satisfeita em %s", timeout)
}

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("freeAddr: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

// ---------------------------------------------------------------------------
// HTTP — contrato da aplicação
// ---------------------------------------------------------------------------

type moneyBody struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

type balanceEnvelope struct {
	Balance moneyBody `json:"balance"`
}

func httpGet(ctx context.Context, url string) (int, []byte) {
	return httpRound(ctx, http.MethodGet, url, "", nil)
}

func httpGetJSON(ctx context.Context, url, token string) (int, []byte) {
	return httpRound(ctx, http.MethodGet, url, token, nil)
}

func httpPostJSON(ctx context.Context, url, token, body string) (int, []byte) {
	return httpRound(ctx, http.MethodPost, url, token, []byte(body))
}

func httpRound(ctx context.Context, method, rawURL, token string, body []byte) (int, []byte) {
	return httpRoundHdrs(ctx, method, rawURL, token, nil, body)
}

func httpRoundHdrs(ctx context.Context, method, rawURL, token string, headers map[string]string, body []byte) (int, []byte) {
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, rawURL, rd)
	if err != nil {
		return 0, nil
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, nil
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b
}

func openWalletHTTP(t *testing.T, ctx context.Context, httpAddr, token, player string) string {
	t.Helper()
	status, body := httpPostJSON(ctx, "http://"+httpAddr+"/wallets", token,
		fmt.Sprintf(`{"playerId":%q,"initialBalance":{"amount":"100.00","currency":"BRL"}}`, player))
	if status != http.StatusCreated {
		t.Fatalf("open wallet = %d: %s", status, body)
	}
	var res struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(body, &res); err != nil {
		t.Fatalf("open wallet body: %v", err)
	}
	return res.ID
}

func processHTTP(t *testing.T, ctx context.Context, httpAddr, token, walletID, player, txID, round, kind, amount string, wantStatus int) {
	t.Helper()
	status, body := httpRoundHdrs(ctx, http.MethodPost, "http://"+httpAddr+"/wagering/transactions", token,
		map[string]string{"Idempotency-Key": txID},
		[]byte(fmt.Sprintf(`{"externalTransactionId":%q,"playerId":%q,"walletId":%q,"roundId":%q,"gameId":"game-1","kind":%q,"money":{"amount":%q,"currency":"BRL"}}`,
			txID, player, walletID, round, kind, amount)))
	if status != wantStatus {
		t.Fatalf("process (%s) = %d: %s", kind, status, body)
	}
}

func assertBalanceHTTP(t *testing.T, ctx context.Context, httpAddr, token, walletID, want string) {
	t.Helper()
	status, body := httpGetJSON(ctx, "http://"+httpAddr+"/wallets/"+walletID, token)
	if status != http.StatusOK {
		t.Fatalf("get wallet = %d: %s", status, body)
	}
	var res balanceEnvelope
	if err := json.Unmarshal(body, &res); err != nil {
		t.Fatalf("get wallet body: %v", err)
	}
	if res.Balance.Amount != want {
		t.Fatalf("saldo = %s, want %s", res.Balance.Amount, want)
	}
}

// ---------------------------------------------------------------------------
// SQS — produção e consumo reais
// ---------------------------------------------------------------------------

func enqueueBet(t *testing.T, ctx context.Context, client messaging.SQSClient, queueURL, walletID, player, txID, messageID, round, amount string) {
	t.Helper()
	envelope := messaging.InboundEnvelope{
		MessageID:  messageID,
		Type:       "WagerTransaction",
		OccurredAt: time.Now().UTC(),
		Data: messaging.InboundWagerData{
			ProviderID:            "provider-a",
			ExternalTransactionID: txID,
			IdempotencyKey:        "provider-a:" + txID,
			PlayerID:              player,
			WalletID:              walletID,
			RoundID:               round,
			GameID:                "game-1",
			Kind:                  "BET",
			Money:                 messaging.MoneyJSON{Amount: amount, Currency: "BRL"},
			CorrelationID:         "corr-" + messageID,
		},
	}
	wire, err := messaging.MarshalInbound(envelope)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	// FIFO: grupo = walletId (ordem por carteira), dedup = messageId (specs §10).
	_, err = client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:               aws.String(queueURL),
		MessageBody:            aws.String(string(wire)),
		MessageGroupId:         aws.String(walletID),
		MessageDeduplicationId: aws.String(messageID),
	})
	if err != nil {
		t.Fatalf("send to input queue: %v", err)
	}
}

func receiveEvents(ctx context.Context, client messaging.SQSClient, queueURL string, max int) ([]event.Envelope, error) {
	out, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:              aws.String(queueURL),
		MaxNumberOfMessages:   10,
		WaitTimeSeconds:       2,
		MessageAttributeNames: []string{"All"},
	})
	if err != nil {
		return nil, err
	}
	var events []event.Envelope
	for _, msg := range out.Messages {
		var env event.Envelope
		if err := json.Unmarshal([]byte(*msg.Body), &env); err != nil {
			continue
		}
		events = append(events, env)
		if max > 0 && len(events) >= max {
			break
		}
	}
	return events, nil
}

func itoa(n int64) string {
	return fmt.Sprintf("%d", n)
}
