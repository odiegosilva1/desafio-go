SHELL := /bin/bash
GOFORMAT_FILES := $(shell gofmt -l .)

.PHONY: fmt vet build test test-race check up down

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
	docker compose up --build

down:
	docker compose down