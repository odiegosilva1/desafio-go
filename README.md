# Wallet Service — transações de apostas com integridade financeira e processamento idempotente

Serviço em Go (Uber Fx) para processamento distribuído de apostas com garantias
financeiras: Money sem ponto flutuante, ledger append-only, idempotência
persistente, transactional outbox/inbox, concorrência por carteira,
reversões seguras e autenticação OAuth 2.0/OIDC via Keycloak.

> As decisões técnicas (dinheiro, transações, idempotência, locks, referências
> pendentes, reversões, inbox/outbox, autenticação/autorização, Fx e shutdown)
> estão em [`ARCHITECTURE.md`](ARCHITECTURE.md).

## 1. Pré-requisitos

A partir de um checkout limpo você precisa de:

| Ferramenta | Versão mínima | Observação |
| --- | --- | --- |
| Go | `1.25.0` | exigida pelo `go.mod` |
| Docker + Docker Compose | Compose v2 | sobe `postgres`, `localstack` e `keycloak` |
| `make` | — | atalhos usados nesta documentação |
| `jq` | — | apenas nos exemplos de chamadas (§5) |

Nenhum segredo é necessário: os valores locais estão em
[`.env.example`](.env.example) e os serviços provisionam as próprias
dependências (`make up` cria o realm Keycloak e as filas SQS).

## 2. Estrutura do repositório

```
cmd/server/               entrada da aplicação e sinalização
cmd/migrate/              CLI de migrations (up|down)
internal/domain/          domínio puro (money, ledger, wagering, wallet, event, derr)
internal/application/     casos de uso, referências, outbox e reconciliação
internal/storage/         port (interfaces) + postgres (pgx, migrações)
internal/httpapi/         handlers HTTP, middlewares de auth e contratos
internal/auth/            validação OIDC e autorização por provedor
internal/messaging/       consumidor SQS + inbox, publisher, envelopes e provisionamento
internal/app/             módulos Fx, ciclo de vida e migrations embutidas
internal/observability/   logs JSON, métricas e health checks
deploy/keycloak/          realm do IdP (identidades de teste)
deploy/localstack/        provisionamento das filas SQS
test/faults/              simulações de falha (Postgres real + fakeSQS)
test/multiinstance/       cenário multi-instância (processos reais)
test/realstack/           malha real (LocalStack + Keycloak, sem fakes)
```

## 3. Configuração — variáveis de ambiente

As variáveis são documentadas em [`.env.example`](.env.example) e lidas pelo
serviço na inicialização (nenhum segredo é embutido no código):

| Grupo | Variáveis | Observação |
| --- | --- | --- |
| HTTP | `HTTP_ADDR` (default `:8080`) | endereço do servidor |
| Banco | `DATABASE_URL` | Postgres `wallet/wallet@localhost:5432/wallet` |
| OIDC | `OIDC_ISSUER_URL`, `OIDC_CLIENT_ID`, `OIDC_CLIENT_SECRET`, `OIDC_INTERNAL_CLIENT_ID` | Keycloak realm `wallet` |
| AWS/SQS | `AWS_ENDPOINT_URL`, `AWS_REGION`, `AWS_SQS_QUEUE`, `AWS_SQS_DLQ`, `AWS_SQS_EVENT_QUEUE`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | LocalStack |
| Workers | `WORKER_REFERENCE_INTERVAL`, `WORKER_OUTBOX_INTERVAL`, `WORKER_OUTBOX_BATCH`, `WORKER_SQS_POLL_INTERVAL`, `SHUTDOWN_TIMEOUT` | cadência dos workers e prazo de shutdown |
| Observabilidade | `LOG_LEVEL` | nível do logger estruturado |

Para desenvolvimento, copie o exemplo localmente:

```sh
cp .env.example .env
export $(grep -v '^#' .env | xargs)   # com go run local, ou deixe a aplicação
```

## 4. Reprodução a partir de um checkout limpo

Os passos abaixo levam do clone à aplicação rodando.

### 4.1. Infraestrutura (Postgres, LocalStack e Keycloak)

```sh
make infra    # docker compose up -d postgres localstack keycloak
```

Sobe apenas os serviços de infraestrutura. Para a stack completa (aplicação
também containerizada, com build da imagem), use o comando exigido pela
entrega ou seu equivalente:

```sh
docker compose up --build   # aplicação + Postgres + LocalStack + Keycloak
make up                      # equivalente: up -d + provisionamento (realm + filas)
```

### 4.2. Migrations (aplicar e reverter)

As migrations são aplicadas automaticamente no start da aplicação. Para
controlar manualmente, há uma CLI:

```sh
make migrate             # aplica as migrations (up, default)
# migrations aplicadas com sucesso
make migrate ARGS=down   # reverte até a versão anterior
# migration 0001 revertida com sucesso
```

O estado do schema é versionado em `schema_version`; as migrações ficam em
`internal/app/migrations/` (`0001_init.up.sql` / `0001_init.down.sql`).

### 4.3. Inicialização das filas SQS

As filas FIFO (`wager-transactions.fifo`, DLQ `wager-transactions-dlq.fifo` e
`wallet-events.fifo`) são criadas de forma **idempotente** no start da
aplicação. Para provisioná-las à parte (ou reparar) no LocalStack:

```sh
make provision          # cria/verifica as filas e o redrive (maxReceiveCount=5)
```

### 4.4. Provisionamento automático do IdP

O Keycloak importa o realm `wallet` automaticamente no primeiro boot a partir
de `deploy/keycloak/realm-export.json` (montado no container), criando os
clientes e identidades de teste — sem passo manual. As identidades e os
fluxos autenticados estão descritos em §5.

### 4.5. Execução da aplicação

```sh
make up      # (opcional) sobe a aplicação containerizada + infra
make run     # go run ./cmd/server, usando as variáveis de .env/.env.example
```

Com a aplicação de pé: health checks em `/health/live` e `/health/ready`,
métricas Prometheus em `/metrics` e a API em `http://localhost:8080`.

## 5. Fluxos autenticados (Keycloak) e exemplos de chamadas

Identidades de teste provisionadas pelo realm (valores locais):

| Identidade | Tipo | Segredo/credencial | Enxerga |
| --- | --- | --- | --- |
| `provider-a` | cliente OAuth (provedor A) | `provider-a-secret` | só carteiras/transações de `provider-a` |
| `provider-b` | cliente OAuth (provedor B) | `provider-b-secret` | só carteiras/transações de `provider-b` |
| `wallet-service-internal` | cliente interno | `wallet-internal-secret` | abertura/reconciliação (não é provedor) |
| `tester` / `tester` | usuário (password grant) | senha `tester` | fluxo interativo via `wallet-service` |

Obter um token de provedor (`client_credentials`) e inspecionar o JWT:

```sh
TOKEN_A="$(curl -s -X POST http://localhost:8081/realms/wallet/protocol/openid-connect/token \
  -d 'grant_type=client_credentials' \
  -d 'client_id=provider-a' \
  -d 'client_secret=provider-a-secret' \
  | jq -r .access_token)"
echo "$TOKEN_A" | cut -d. -f2 | base64 -d 2>/dev/null | jq {client_id,scope,aud} || true
```

Abrir uma carteira (o `client_id` do token define o `providerId`):

```sh
curl -s http://localhost:8080/wallets \
  -H "Authorization: Bearer $TOKEN_A" -H 'Content-Type: application/json' \
  -d '{"playerId":"player-1","initialBalance":{"amount":"100.00","currency":"BRL"}}'
# {"id":"<walletId>","playerId":"player-1","balance":{"amount":"100.00","currency":"BRL"},"version":0}
```

Enviar uma aposta (a chave de idempotência vai no header `Idempotency-Key`) e
consultar o resultado:

```sh
WALLET="<walletId>"
curl -s http://localhost:8080/wagering/transactions \
  -H "Authorization: Bearer $TOKEN_A" -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: tx-1' \
  -d "{\"externalTransactionId\":\"tx-1\",\"playerId\":\"player-1\",\"walletId\":\"$WALLET\",
        \"roundId\":\"round-1\",\"gameId\":\"fortune-chimp\",\"kind\":\"BET\",
        \"money\":{\"amount\":\"25.00\",\"currency\":\"BRL\"}}"
# {"transactionId":"...","status":"PROCESSED","balance":{"amount":"75.00","currency":"BRL"},...}

curl -s http://localhost:8080/wallets/$WALLET \
  -H "Authorization: Bearer $TOKEN_A"
curl -s http://localhost:8080/wallets/$WALLET/ledger \
  -H "Authorization: Bearer $TOKEN_A"
```

Isolamento por provedor, reconciliação com o cliente interno e fluxo
interativo com o usuário de teste:

```sh
# provedor B (token próprio)
TOKEN_B="$(curl -s -X POST http://localhost:8081/realms/wallet/protocol/openid-connect/token \
  -d 'grant_type=client_credentials' \
  -d 'client_id=provider-b' \
  -d 'client_secret=provider-b-secret' \
  | jq -r .access_token)"

# provider-b não vê a carteira de provider-a
curl -s -o /dev/null -w '%{http_code}\n' http://localhost:8080/wallets/$WALLET \
  -H "Authorization: Bearer $TOKEN_B"    # 404

# reconciliação (somente wallet-service-internal)
TOKEN_INT="$(curl -s -X POST http://localhost:8081/realms/wallet/protocol/openid-connect/token \
  -d 'grant_type=client_credentials' -d 'client_id=wallet-service-internal' \
  -d 'client_secret=wallet-internal-secret' | jq -r .access_token)"
curl -s -X POST http://localhost:8080/wallets/$WALLET/reconciliation \
  -H "Authorization: Bearer $TOKEN_INT" -H 'Content-Type: application/json' -d '{}'

# usuário de teste (password grant, somente demonstração interativa)
curl -s -X POST http://localhost:8081/realms/wallet/protocol/openid-connect/token \
  -d 'grant_type=password' -d 'client_id=wallet-service' \
  -d 'client_secret=wallet-service-secret' -d 'username=tester' -d 'password=tester'
```

## 6. Testes

### 6.1. Gate do projeto (comandos da entrega)

Os comandos exigidos pela entrega e seus equivalentes via `make`:

```sh
gofmt -l .                 # código formatado (or: make fmt)
go vet ./...               # análise estática (or: make vet)
go build ./...             # compilação (or: make build)
go test ./...              # unit (or: make test)
go test -race ./...        # unit com detector de corrida (or: make test-race)
make check                 # todos os anteriores em um gate
```

### 6.2. Testes de integração (build tag `integration`)

Exigem o Postgres local (default `postgres://wallet:wallet@localhost:5432/wallet`).
São serializados (`-p 1`) porque exercitam o mesmo banco:

```sh
make db-up                                # prepara a dependência (Postgres)
go test -tags integration -count=1 -p 1 ./internal/app/... ./internal/application/ ./internal/storage/... ./test/...
make test-integration                     # o mesmo, via target
make integration-check                    # db-up + integração + race + db-down
make test-race-integration                # integração com detector de corrida
```

Suítes agrupadas sob `./test/...`:

- `test/realstack` — malha real, sem fakes (LocalStack + Keycloak + PostgreSQL).
  Se a infra de Docker não estiver de pé, o teste escreve **SKIP**.
- `test/multiinstance` — três processos reais disputando o mesmo banco.
- `test/faults` — falhas do consumidor/outbox, concorrência e reversões.

### 6.3. Malha real, sem fakes (`test/realstack`)

`TestRealStackEndToEnd` inicia a **aplicação de produção** (`app.New`) contra
PostgreSQL, **LocalStack (SQS)** e **Keycloak (OIDC)** reais — nada de
`fakeSQS`/`fakeVerifier`:

```sh
make test-realstack   # sobe LocalStack+Keycloak, provisiona as filas e testa
```

Cobra a evidência das garantias exigidas: autenticação efetiva por token real,
abertura de carteira, aposta via HTTP, aposta via **consumidor SQS real**
(inbox deduplica reentregas), eventos da **transactional outbox publicados no
destino** `wallet-events.fifo` e reconciliação consistente com o cliente
interno autenticado. Requer Postgres disponível (`make db-up` ou `make up`).

### 6.4. Cenário multi-instância (`test/multiinstance`)

`TestMultiInstanceIndependenceAndRecovery` demonstra as garantias de escala
horizontal: três **processos independentes** (os/exec do próprio binário de
teste, cada um com pool de conexões, outbox publisher, reference worker e
memória próprios) disputando o mesmo banco apenas por `FOR UPDATE SKIP LOCKED`.
Valida carteiras paralelas, serialização da mesma carteira sob concorrência
forte, rejeição por saldo, publicação da outbox exatamente uma vez por eventId
no agregado das instâncias, e retomada de trabalho abandonado
(`PENDING_REFERENCE` + outbox) após a morte brutal de uma instância.

### 6.5. Simulações de falha (`test/faults`)

Usam o **Postgres real** e um `fakeSQS` que implementa a interface
`messaging.SQSClient` (sem LocalStack), dirigindo o `Consumer` e o
`OutboxPublisher` de produção contra falhas:

- `TestConsumerDBOutageKeepsMessageAndRecovers` — banco indisponível durante o
  processamento: a mensagem **permanece** na fila (sem delete, sem DLQ) e, com o
  banco recuperado, a reentrega processa exatamente-uma-vez (inbox deduplica).
- `TestConsumerBusinessRejectionIsTerminalAndNoDLQ` — rejeição definitiva de
  negócio (saldo insuficiente) é **terminal**: consome, registra na inbox com o
  código e **não** encaminha à DLQ.
- `TestConsumerInvalidPayloadToDLQ` — corpo ilegível vai à DLQ com
  `INVALID_MESSAGE` e é removido da fila, sem efeito no banco.
- `TestOutboxPublisherRetryPublishesExactlyOnce` — destino de eventos fora do
  ar: registros permanecem `PENDING` (rollback, nenhum `MarkPublished` com
  falha) e, recuperado, cada eventId é entregue com id estável e nenhuma
  republicação após a confirmação.
- `TestOutboxPublisherSurvivesInstanceChange` — outra instância retoma registros
  herdados sem republicar os já confirmados (`published_by` por instância).
- `TestSameOperation50ParallelSingleDebit` — a MESMA aposta enviada 50 vezes em
  paralelo, com pools de conexões e memória independentes (instâncias
  distintas), produz **exatamente um débito**, saldo 100.00→75.00 e 49 replays
  idempotentes.
- `TestHTTPAndSQSShareIdempotency` — a MESMA operação cruza **HTTP e SQS**: a
  primeira movimenta uma única vez; a entrada SQS com a mesma chave e conteúdo é
  deduplicada pela inbox + idempotência persistente, sem débito duplicado e sem
  DLQ.
- `TestTwoEqualBetsOverBalance` — duas apostas de 80.00 disputando um saldo de
  100.00 → exatamente uma `PROCESSED` e a outra `REJECTED`
  (`INSUFFICIENT_FUNDS`), **1 lançamento** e saldo final 20.00.
- `TestConsumerCrashAfterCommitRedeliversOnce` — consumidor morre entre o
  commit e o delete: a reentrega é deduplicada pela inbox, sem segundo débito.
- `TestRefundThenRollbackSameBetRejected` / `TestRollbackWinTwiceRejected` —
  uma referência nunca sofre duas reversões processadas com o mesmo efeito
  financeiro (`DUPLICATE_REVERSAL`), nos dois sentidos (crédito e débito).
- `TestRollbackOutOfOrderResolvedByWorker` / `TestPendingReferenceExpiresRejected`
  — ROLLBACK enviado antes da referência entra em `PENDING_REFERENCE` e o worker
  resolve ao chegar a referência; referência que nunca chega esgota o TTL e
  termina `REJECTED`/`REFERENCE_NOT_FOUND`, sem efeito.
- `TestReplayAfterRestart` — nova instância sobre o mesmo banco reproduz o replay
  idempotente e retoma pendências de referência (morte/restart do processo).
- `TestIdempotencyConflictDifferentPayload` — mesma chave com conteúdo diferente
  é `IDEMPOTENCY_CONFLICT`, sem efeito; conteúdo idêntico segue replay.
- `TestCurrencyMismatchNoFinancialEffect`, `TestRefundRequiresProcessedBet`,
  `TestOpeningEmitsWalletEvents` — moeda divergente sem efeito financeiro,
  reversões exigem referência processada e a abertura publica os eventos.

Contrato de DLQ (validado nos testes): apenas **falha permanente**, **payload
inválido** ou tentativas esgotadas vão à DLQ; rejeições de negócio são
confirmadas na inbox e removidas da fila.

## 7. Contrato HTTP (resumo)

| Status | Situação |
| --- | --- |
| `200` | operação concluída / leituras / reconciliação consistente |
| `201` | carteira criada |
| `202` | operação aguardando referência (`PENDING_REFERENCE`) |
| `400` | entrada inválida (`INVALID_PAYLOAD`, `INVALID_MONEY`, `MISSING_IDEMPOTENCY_KEY`, `OPENING_BLOCKED`, cursor/limit) |
| `404` | carteira/transação não encontrada — inclusive fora do provedor autenticado |
| `409` | conflito de idempotência (mesma chave, payload diferente) ou abertura duplicada |
| `422` | rejeição de negócio terminal (`INSUFFICIENT_FUNDS`, `DUPLICATE_REVERSAL`, moeda incompatível, …) |
| `503` | indisponibilidade transitória (retry do cliente) |

Corpo de erro: `{"code":"<failureCode>","message":"..."}`. O mapeamento
completo (status × classes de domínio × `failureCode`) está em
`ARCHITECTURE.md` §Contrato HTTP.

## 8. Mensageria e recuperação (resumo)

- **Consumidor SQS**: visibility `30s`; falha transitória estende a visibilidade
  com backoff exponencial (`ChangeMessageVisibility`, `5s*2^(n-1)`, teto `600s`);
  nada é apagado antes do commit; a inbox deduplica reentregas (identidade =
  `messageId` do envelope, `ON CONFLICT DO NOTHING`); redrive com
  `maxReceiveCount = 5` para a DLQ; rejeição de negócio é terminal (sem DLQ).
- **Transactional outbox**: publica somente após o commit, com backoff durável
  por registro, `MarkPublished` idempotente e lote que não é interrompido por
  uma falha individual.
- **Shutdown**: `SIGTERM` encerra em ordem, `SHUTDOWN_TIMEOUT` garante término
  observável e reentrega segura.

## 9. Arquitetura e decisões

`ARCHITECTURE.md` registra as decisões de: modelo financeiro (Money/ledger),
idempotência, concorrência por carteira e escala horizontal, referências
pendentes e reversões, inbox/outbox e wire FIFO, autenticação/autorização,
composição Fx e shutdown, observabilidade, migrações, e as limitações /
interpretações adotadas / trabalho não concluído.

## 10. Desenvolvimento (Git Flow)

- `main` — estável, apenas via `release`/`hotfix`, com tags semver.
- `develop` — integração das `feature/*`.
- `feature/*` — desenvolvimento, finalizadas com merge `--no-ff`.
- `release/v*` — preparação de versão.
- `hotfix/*` — correções urgentes sobre `main`.

```sh
make check   # gate local antes de finalizar qualquer branch
```

## Comandos de referência rápida

```sh
make check              # gofmt, vet, build, test, race
make up                 # sobe a stack completa e provisiona (reino + filas)
make infra              # apenas Postgres + LocalStack + Keycloak
make run                # go run ./cmd/server (com .env dev)
make provision          # cria as filas SQS no LocalStack (idempotente)
make db-up / db-down    # sobe/derruba apenas o Postgres
make migrate            # aplica migrations (up, default)
make migrate ARGS=down  # reverte migrations (até a versão anterior)
make test-integration   # integração (exige Postgres; usa -p 1)
make integration-check  # db-up + integração + race + db-down
make test-realstack     # malha REAL: LocalStack+Keycloak sem fakes (exige Docker)
make e2e                # up + run (malha completa HTTP+SQS+Keycloak)
make down               # derruba a infra
```