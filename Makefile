SHELL := /bin/bash
GOFORMAT_FILES := $(shell gofmt -l .)
export PATH := $(HOME)/.local/bin:$(PATH)
export AWS_ENDPOINT_URL ?= http://localhost:4566

.PHONY: fmt vet build test test-race check run up infra down db-up db-down migrate test-integration test-race-integration integration-setup integration-teardown integration-check provision e2e e2e-clean

fmt:
	@if [ -n "$(GOFORMAT_FILES)" ]; then \
		echo "Arquivos fora do gofmt:"; \
		echo "$(GOFORMAT_FILES)"; \
		exit 1; \
	fi
	@echo "gofmt OK"

vet:
	go vet ./...

build:
	go build ./...

test:
	go test ./...

test-race:
	go test -race ./...

check: fmt vet build test test-race
	@echo "Gate local OK (fmt, vet, build, test, test-race)"

# Subir o serviço com a configuração local (requer infra via `make up`).
run:
	go run ./cmd/server

up:
	docker compose up --build -d
	$(MAKE) provision

# Apenas a infraestrutura (sem a aplicação containerizada), para desenvolver
# com `make run` localmente.
infra:
	docker compose up --build -d postgres localstack keycloak
	$(MAKE) provision

down:
	docker compose down

# --- Infra local (Postgres) ---
db-up:
	docker compose up -d postgres
	docker compose exec -T postgres sh -c 'until pg_isready -U wallet -d wallet; do sleep 1; done'
	@echo "PG pronto"

db-down:
	docker compose stop postgres

# --- Migrations ---
# Aplicam automaticamente no start; útil para reverter o schema em dev.
migrate:
	go run ./cmd/migrate

# --- SQS: provisionamento idempotente das filas no LocalStack ---
provision:
	bash deploy/localstack/provision-sqs.sh

# --- Testes de integração (build tag `integration`; exigem Postgres up) ---
# Serializados (-p 1): os pacotes resetam o mesmo banco local.
integration-setup: db-up
	@echo "Infra de integração pronta (Postgres up)"

test-integration:
	go test -tags integration -count=1 -p 1 ./internal/app/... ./internal/application/ ./internal/storage/... ./test/...

test-race-integration:
	go test -race -tags integration -count=1 -p 1 ./internal/app/... ./internal/application/ ./internal/storage/... ./test/...

integration-teardown: db-down
	@echo "Infra de integração derrubada"

integration-check: integration-setup test-integration test-race-integration integration-teardown
	@echo "Integração OK (pg real, race, composição Fx)"

# --- E2E: malha completa (HTTP + SQS + Keycloak + outbox) ---
e2e: up run

e2e-clean: down
	@echo "E2E encerrado"