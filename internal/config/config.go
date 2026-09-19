// Package config carrega e valida a configuração da aplicação a partir de
// variáveis de ambiente. Valores locais de exemplo em .env.example. Nenhum
// segredo é embutido no código; segredos reais entram por variável de ambiente.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config é a configuração validada da aplicação.
type Config struct {
	HTTPAddr    string
	DatabaseURL string

	OIDC OIDCConfig

	AWS AWSConfig

	Workers WorkersConfig

	LogLevel string

	// ShutdownTimeout é o prazo concedido para término observável dos workers
	// e do servidor no encerramento do processo (SIGTERM).
	ShutdownTimeout time.Duration
}

// OIDCConfig parametriza a validação de tokens contra o IdP externo.
type OIDCConfig struct {
	// IssuerURL é o emissor do IdP (ex.: http://localhost:8081/realms/wallet).
	IssuerURL string
	// ClientID é o cliente do próprio serviço, aceito como audience.
	ClientID string
	// ClientSecret é o segredo do serviço (client_credentials para exigir
	// identificação do próprio serviço quando aplicável).
	ClientSecret string
	// InternalClientID é o client_id do serviço interno de carteira; tokens
	// desse cliente são tratados como identidade interna (reconciliação).
	InternalClientID string
}

// AWSConfig parametriza o acesso ao SQS (LocalStack ou AWS real).
type AWSConfig struct {
	EndpointURL string
	Region      string
	// QueueURL ou nome da fila de entrada de operações.
	InputQueue string
	// DLQ é a dead-letter queue das operações com falha permanente.
	DLQ string
	// EventQueue é o destino dos eventos publicados pela outbox.
	EventQueue string
	AccessKey  string
	SecretKey  string
}

// WorkersConfig parametriza os workers de integração.
type WorkersConfig struct {
	// ReferenceInterval é o intervalo de varredura do worker de referências.
	ReferenceInterval time.Duration
	// OutboxInterval é o intervalo de varredura do publisher da outbox.
	OutboxInterval time.Duration
	// OutboxBatchSize é o máximo de registros disputados por passada.
	OutboxBatchSize int
	// SQSPollInterval é o intervalo entre polos do consumidor SQS.
	SQSPollInterval time.Duration
}

// Load lê a configuração das variáveis de ambiente e valida os valores.
func Load() (Config, error) {
	c := Config{
		HTTPAddr:    env("HTTP_ADDR", ":8080"),
		DatabaseURL: env("DATABASE_URL", "postgres://wallet:wallet@localhost:5432/wallet?sslmode=disable"),
		OIDC: OIDCConfig{
			IssuerURL:        env("OIDC_ISSUER_URL", ""),
			ClientID:         env("OIDC_CLIENT_ID", "wallet-service"),
			ClientSecret:     env("OIDC_CLIENT_SECRET", ""),
			InternalClientID: env("OIDC_INTERNAL_CLIENT_ID", "wallet-service-internal"),
		},
		AWS: AWSConfig{
			EndpointURL: env("AWS_ENDPOINT_URL", ""),
			Region:      env("AWS_REGION", "us-east-1"),
			InputQueue:  env("AWS_SQS_QUEUE", "wager-transactions.fifo"),
			DLQ:         env("AWS_SQS_DLQ", "wager-transactions-dlq.fifo"),
			EventQueue:  env("AWS_SQS_EVENT_QUEUE", "wallet-events.fifo"),
			AccessKey:   env("AWS_ACCESS_KEY_ID", ""),
			SecretKey:   env("AWS_SECRET_ACCESS_KEY", ""),
		},
		Workers: WorkersConfig{
			ReferenceInterval: envDuration("WORKER_REFERENCE_INTERVAL", 2*time.Second),
			OutboxInterval:    envDuration("WORKER_OUTBOX_INTERVAL", 1*time.Second),
			OutboxBatchSize:   envInt("WORKER_OUTBOX_BATCH", 32),
			SQSPollInterval:   envDuration("WORKER_SQS_POLL_INTERVAL", 5*time.Second),
		},
		LogLevel:        env("LOG_LEVEL", "info"),
		ShutdownTimeout: envDuration("SHUTDOWN_TIMEOUT", 25*time.Second),
	}

	if err := c.validate(); err != nil {
		return Config{}, err
	}
	return c, nil
}

// LoadAWS lê apenas a subconfiguração de AWS das variáveis de ambiente, sem as
// validações de PostgreSQL/OIDC. Usada pelas ferramentas de operação
// (cmd/producer e o replay da DLQ) que não dependem do IdP nem do banco.
func LoadAWS() AWSConfig {
	return AWSConfig{
		EndpointURL: env("AWS_ENDPOINT_URL", ""),
		Region:      env("AWS_REGION", "us-east-1"),
		InputQueue:  env("AWS_SQS_QUEUE", "wager-transactions.fifo"),
		DLQ:         env("AWS_SQS_DLQ", "wager-transactions-dlq.fifo"),
		EventQueue:  env("AWS_SQS_EVENT_QUEUE", "wallet-events.fifo"),
		AccessKey:   env("AWS_ACCESS_KEY_ID", ""),
		SecretKey:   env("AWS_SECRET_ACCESS_KEY", ""),
	}
}

func (c Config) validate() error {
	var errs []string
	if strings.TrimSpace(c.DatabaseURL) == "" {
		errs = append(errs, "DATABASE_URL é obrigatório")
	}
	if strings.TrimSpace(c.OIDC.IssuerURL) == "" {
		errs = append(errs, "OIDC_ISSUER_URL é obrigatório")
	}
	if strings.TrimSpace(c.OIDC.ClientID) == "" {
		errs = append(errs, "OIDC_CLIENT_ID é obrigatório")
	}
	if strings.TrimSpace(c.AWS.Region) == "" {
		errs = append(errs, "AWS_REGION é obrigatório")
	}
	if strings.TrimSpace(c.AWS.InputQueue) == "" {
		errs = append(errs, "AWS_SQS_QUEUE é obrigatório")
	}
	if c.Workers.OutboxBatchSize <= 0 {
		errs = append(errs, "WORKER_OUTBOX_BATCH deve ser > 0")
	}
	if len(errs) > 0 {
		return fmt.Errorf("config: %s", strings.Join(errs, "; "))
	}
	return nil
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	if d, err := time.ParseDuration(v); err == nil {
		return d
	}
	return def
}

func envInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	if i, err := strconv.Atoi(v); err == nil {
		return i
	}
	return def
}
