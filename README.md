# desafio-go

Serviço em Go (Uber Fx) para processamento distribuído de apostas com garantias
financeiras: Money sem ponto flutuante, ledger append-only, idempotência
persistente, transactional outbox/inbox, concorrência por carteira e
autenticação OAuth 2.0/OIDC via Keycloak.

> Documento em construção. Veja o enunciado completo em [`specs.md`](specs.md) e
> as decisões em [`ARCHITECTURE.md`](ARCHITECTURE.md).

## Estrutura

```
cmd/server/               entrada da aplicação e sinalização
internal/domain/          domínio puro (money, ledger, wagering, wallet, event, derr)
internal/application/     casos de uso, referências, outbox e reconciliação
internal/storage/         port (interfaces) + postgres (pgx, migrações)
internal/httpapi/         contratos HTTP, middlewares de auth e handlers
internal/auth/            validação OIDC e autorização por provedor
internal/messaging/       consumidor SQS + inbox, publisher e provisionamento
internal/app/             módulos Fx, ciclo de vida e migrations embutidas
internal/observability/   logs JSON, métricas e health checks
deploy/                   realm Keycloak e provisionamento de filas SQS
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
make check              # gate unit (fmt, vet, build, test, race)
make up                 # sobe Postgres + LocalStack + Keycloak e provisiona SQS
make run                # go run ./cmd/server (com .env dev)
make provision          # cria as filas SQS no LocalStack (idempotente)
make db-up              # apenas Postgres
make test-integration   # integração (exige Postgres; usa -p 1)
make integration-check  # db-up + integração + race + db-down
make e2e                # up + run (malha completa HTTP+SQS+Keycloak)
make down               # derruba a infra
```

## Configuração

Variáveis de ambiente documentadas em [`.env.example`](.env.example)
(`HTTP_ADDR`, `DATABASE_URL`, `OIDC_*`, `AWS_*`, `WORKER_*`, `SHUTDOWN_TIMEOUT`,
`LOG_LEVEL`). Nenhum segredo é embutido no código.

## Testes de integração

Os testes com build tag `integration` exigem o Postgres local (default
`postgres://wallet:wallet@localhost:5432/wallet`). São serializados
(`-p 1`) porque exercitam o mesmo banco:

```sh
make db-up
go test -tags integration -count=1 -p 1 ./internal/app/... ./internal/application/ ./internal/storage/...
```