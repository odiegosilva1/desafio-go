# desafio-go

Serviço em Go (Uber Fx) para processamento distribuído de apostas com garantias
financeiras: Money sem ponto flutuante, ledger append-only, idempotência
persistente, transactional outbox/inbox, concorrência por carteira e
autenticação OAuth 2.0/OIDC via Keycloak.

> Documento em construção. Veja o enunciado completo em [`specs.md`](specs.md).

## Estrutura

```
cmd/server/             entrada da aplicação e composição Fx
internal/domain/        domínio puro (sem Fx/HTTP/SQS/pgx)
internal/persistence/   repos, migrations e controle de concorrência
internal/http/          roteador, handlers, middlewares e contratos
internal/auth/          validação OIDC e autorização por provedor
internal/messaging/     consumidor SQS, inbox e worker de outbox
internal/app/           módulos Fx e ciclo de vida
internal/observability/ logs JSON, métricas e health checks
internal/reconcile/     reconciliação saldo x ledger
migrations/             SQL versionado (up/down)
deploy/                 realm Keycloak e provisionamento de filas SQS
test/                   integração, concorrência e múltiplas instâncias
```

## Fluxo de trabalho (Git Flow)

- `main` — estável, apenas via `release`/`hotfix`, com tags semver.
- `develop` — integração das `feature/*`.
- `feature/*` — desenvolvimento, finalizadas com merge `--no-ff`.
- `release/v*` — preparação de versão.
- `hotfix/*` — correções urgentes sobre `main`.

Gate de qualidade local antes de finalizar qualquer branch:

```sh
make check   # gofmt -l, go vet, go build, go test, go test -race
```

## Comandos

```sh
make check
make up      # docker compose up --build
```