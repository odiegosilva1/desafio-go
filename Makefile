SHELL := /bin/bash
GOFORMAT_FILES := $(shell gofmt -l .)
export PATH := $(HOME)/.local/bin:$(PATH)

.PHONY: fmt vet build test test-race check up down db-up db-down test-integration test-race-integration integration-setup integration-teardown

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

up:
	docker compose up --build -d

down:
	docker compose down

# Subir apenas o PostgreSQL (para testes de integração) esperando saúde.
db-up:
	docker compose up -d postgres
	docker compose exec -T postgres sh -c 'until pg_isready -U wallet -d wallet; do sleep 1; done'
	@echo "PG pronto"

db-down:
	docker compose stop postgres

# --- Testes de integração (build tag `integration`; exigem Postgres up) ---
integration-setup: db-up
	@echo "Infra de integração pronta (Postgres up)"

test-integration:
	go test -tags integration -count=1 ./internal/storage/...

test-race-integration:
	go test -race -tags integration -count=1 ./internal/storage/...

integration-teardown: db-down
	@echo "Infra de integração derrubada"

integration-check: integration-setup test-integration test-race-integration integration-teardown
	@echo "Integração OK (pg real, race, concorrência e replay)"
