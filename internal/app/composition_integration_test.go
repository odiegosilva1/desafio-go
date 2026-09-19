//go:build integration

package app

import (
	"context"
	"io"
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"

	"desafio-go/internal/auth"
	"desafio-go/internal/messaging"
)

// fakeVerifier substitui o verificador OIDC (que exigiria Keycloak) para o
// teste de composição Fx.
type fakeVerifier struct{}

func (fakeVerifier) Verify(ctx context.Context, rawToken string) (auth.Principal, error) {
	return auth.Principal{}, auth.ErrInvalidToken
}

func freePort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("free port: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

func TestCompositionStartStopIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	url := os.Getenv("DATABASE_URL")
	if url == "" {
		url = "postgres://wallet:wallet@localhost:5432/wallet?sslmode=disable"
	}
	probe, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Skipf("postgres indisponível: %v", err)
	}
	if err := probe.Ping(ctx); err != nil {
		t.Skipf("postgres indisponível: %v", err)
	}
	probe.Close()

	t.Setenv("HTTP_ADDR", freePort(t))
	t.Setenv("OIDC_ISSUER_URL", "http://127.0.0.1:1/realms/wallet")
	t.Setenv("SHUTDOWN_TIMEOUT", "2s")
	t.Setenv("WORKER_REFERENCE_INTERVAL", "100ms")
	t.Setenv("WORKER_OUTBOX_INTERVAL", "100ms")
	t.Setenv("WORKER_OUTBOX_BATCH", "32")
	t.Setenv("WORKER_SQS_POLL_INTERVAL", "200ms")

	var pool *pgxpool.Pool
	appx := New(
		// Overrides apenas das dependências que exigiriam LocalStack/Keycloak.
		// fx.Replace decorates o tipo: o construtor original não é invocado.
		fx.Replace(fx.Annotate(fakeVerifier{}, fx.As(new(auth.Verifier)))),
		func() fx.Option {
			unreachable := "http://127.0.0.1:1/queue/unreachable"
			return fx.Replace(messaging.Queues{
				InputQueueURL: unreachable,
				DLQURL:        unreachable,
				EventQueueURL: unreachable,
			})
		}(),
		fx.NopLogger,
		fx.Populate(&pool),
	)
	if err := appx.Err(); err != nil {
		t.Fatalf("composição Fx inválida: %v", err)
	}

	if err := appx.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}

	// Migrations aplicadas no start.
	var version int
	if err := pool.QueryRow(ctx, `SELECT version FROM schema_version`).Scan(&version); err != nil {
		t.Fatalf("schema_version: %v", err)
	}
	if version != 1 {
		t.Fatalf("schema_version = %d, want 1", version)
	}

	httpAddr := os.Getenv("HTTP_ADDR")
	live, err := httpGet(ctx, "http://"+httpAddr+"/health/live")
	if err != nil {
		t.Fatalf("health/live: %v", err)
	}
	if live != http.StatusOK {
		t.Fatalf("health/live = %d", live)
	}
	ready, err := httpGet(ctx, "http://"+httpAddr+"/health/ready")
	if err != nil {
		t.Fatalf("health/ready: %v", err)
	}
	if ready != http.StatusOK {
		t.Fatalf("health/ready = %d", ready)
	}

	if err := appx.Stop(ctx); err != nil {
		t.Fatalf("stop: %v", err)
	}

	// Após o stop, o servidor HTTP não aceita mais conexões.
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get("http://" + httpAddr + "/health/live")
	if err == nil {
		_ = resp.Body.Close()
		t.Fatal("servidor HTTP ainda aceitou requisição após o stop")
	}
}

func httpGet(ctx context.Context, url string) (int, error) {
	client := &http.Client{Timeout: 5 * time.Second}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode, nil
}
