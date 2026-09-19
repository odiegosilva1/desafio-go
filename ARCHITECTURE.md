# Arquitetura

Serviço distribuído de apostas/carteiras em Go, montado com Uber Fx. Este
documento registra as decisões relevantes e o fluxo de uma operação de ponta a
ponta. O enunciado completo está em [`specs.md`](specs.md).

## Visão geral

```
Provedor de jogos (client_credentials)
        │  JWT (client_id = providerId)
        ▼
   HTTP API (internal/httpapi)          SQS (internal/messaging)
        │                                   │  inbox durável
        ▼                                   ▼
   internal/application (casos de uso) ◄────┤
        │  transação SQL única (outbox+inbox no mesmo commit)
        ▼
   PostgreSQL (internal/storage/postgres)  ◄── migrations embutidas (internal/app/migrations)
```

Processamento financeiro: HTTP (baixa latência) e SQS (desacoplado) convergem
no **mesmo** caso de uso (`application.Service`) e no **mesmo** SQL transacional.
HTTP e SQS produzem hashes de payload idênticos (`newPendingTransaction`), o
que torna o replay idempotente nas duas portas.

## Pacotes

| Pacote | Responsabilidade |
| --- | --- |
| `internal/domain/*` | Domínio puro (money, ledger, wagering, wallet, event, derr). Sem Fx/HTTP/SQS/pgx. |
| `internal/application` | Casos de uso e regras de integração (open, process, reconcile, tokens). |
| `internal/storage/port` | Interfaces do armazenamento (Ports). |
| `internal/storage/postgres` | Adapters (pgx), migrações e concorrência SQL. |
| `internal/httpapi` | Contratos HTTP, middlewares de auth e handlers. |
| `internal/auth` | Validação OIDC e modelo de autorização por provedor. |
| `internal/messaging` | Consumidor SQS + inbox, publisher da outbox, provisionamento de filas. |
| `internal/observability` | Logs JSON, métricas e health checks. |
| `internal/app` | Grafo Fx: providers, lifecycle e migrations embutidas. |
| `cmd/server` | Bootstrap e sinalização (SIGTERM/SIGINT). |

## Dependência e inversão

- `storage/port` define interfaces; `storage/postgres` implementa (estado).
  Nenhum domínio importa `pgx` ou `SQS`.
- No grafo Fx, construtores de storage são anotados com `fx.Annotate(..., fx.As(new(port.X)))`
  para serem consumidos como portas.
- `httpapi.Service`, `application.OutboundPublisher` e `auth.Verifier` são
  interfaces injetadas via `fx.As`; o teste de composição substitui verifier e
  filas por fakes com `fx.Replace` (start/stop sem LocalStack/Keycloak).

## Modelo financeiro

- **Money**: `int64` em "units" (centavos). Nunca ponto flutuante. Conversões
  apenas em fronteiras (parsing/validação).
- **Ledger**: append-only; cada lançamento tem `balance_before`/`balance_after`
  coerentes com o saldo corrente; UPDATE/DELETE bloqueados por trigger
  (`prevent_ledger_modification`); unicidade `(wallet_id, transaction_id)`.
- **Saldo nunca negativo**: constraint `CHECK (balance_units >= 0)` e guarda no
  agregado.
- Saldo da carteira é a fonte da verdade; o ledger é a auditoria e a base da
  reconciliação.

## Concorrência por carteira

- `Get` de carteira usa `SELECT ... FOR UPDATE` dentro da transação do caso de
  uso. Debitar/creditar só ocorre sob esse lock; portanto não há atualização
  concorrente perdida entre instâncias.
- `UpdateBalance` ainda valida `version = versão_lida+1` (esperança otimista):
  se filas de escrita não respeitarem o lock, o conflito é detectado e o caso
  de uso classifica como `transient` (retry).
- Sob alta concorrência na mesma carteira, inserções paralelas nos mesmos
  índices únicos podem gerar impasses (SQLSTATE 40P01). A UOW converte
  `40001`/`40P01` em `ErrConcurrentUpdate` (`mapRetryable`), e o caso de uso
  retenta com relock por carteira um número limitado de vezes; o consumidor
  SQS ainda reentrega a mensagem após falha transitória.

## Escala horizontal (múltiplas instâncias)

- Instâncias são independentes: cada uma mantém o próprio pool de conexões,
  publisher de outbox, reference worker e memória; coordenam-se **apenas** pelo
  banco (`FOR UPDATE SKIP LOCKED`). Não há lock distribuído nem dependência de
  instância única.
- Validado por `test/multiinstance` (specs linhas 200/415): três processos
  reais (os/exec) disputando o mesmo Postgres — carteiras paralelas, mesma
  carteira sob concorrência forte, publicação da outbox exatamente uma vez por
  eventId no agregado e retomada de trabalho abandonado após morte brutal de
  uma instância.

## Idempotência e integração

- **Idempotência persistente por provedor**: `UNIQUE (provider_id,
  idempotency_key)` e `UNIQUE (provider_id, external_tx_id)`. Replays devolvem
  o estado persistido (`IdempotentReplay`), sem duplicação financeira; payload
  diferente com a mesma chave → conflito (`404`/rejeição).
- **Transactional outbox**: eventos (`WagerTransactionProcessed`,
  `WalletBalanceChanged`, `WagerTransactionRejected`, `ReferencePending`,
  `Opening...`) são gravados no **mesmo commit** da operação. O publisher
  reivindica lotes com `FOR UPDATE SKIP LOCKED` e publica em
  `wallet-events.fifo` usando `MessageDeduplicationId = eventId`
  (republicações não duplicam) e `MessageGroupId = aggregateId`.
- **Transactional inbox**: o consumidor SQS registra a mensagem e processa a
  operação **na mesma transação**; commit único garante exactly-once no broker+
  banco (reentrega vira duplicata da inbox/replay idempotente).

## Reversões e PENDING_REFERENCE

- **REFUND** sobre BET e **ROLLBACK** sobre BET/WIN/REFUND, com validação de
  provedor/jogador/carteira/moeda/rodada/valor e rejeição de reversão
  duplicada do mesmo tipo.
- Operação que referencia uma transação ainda inexistente/em andamento não
  falha: entra em **PENDING_REFERENCE** com `next_attempt_at` em backoff
  exponencial e é retomada pelo `ReferenceWorker` (múltiplas instâncias
  disputam com `FOR UPDATE SKIP LOCKED`).
- **TTL**: `MaxReferenceAttempts = 5`; esgotado → `REJECTED`.

## Autenticação e autorização

- OAuth 2.0/OIDC `client_credentials` no Keycloak. O `client_id` do JWT define
  o **providerId autenticado**: o provedor só enxerga suas próprias carteiras
  e transações (isolamento 404).
- Cliente interno (`wallet-service-internal`) e escopos
  `wallet:provider` / `wallet:internal` / `wallet:admin` habilitam
  abertura/reconciliação internas; `X-Wallet-Internal-Api-Key` protege a via
  interna (não pública).
- Health checks públicos. Métricas Prometheus em `/metrics`.

## Migrações

- `internal/app/migrations/0001_init.up.sql` espelha exatamente o SQL dos
  adapters (wallets, wallet_ledger, wagering_transactions, outbox, inbox,
  schema_version). Aplicadas no `OnStart` do Fx (`fs.Sub` sobre o `embed`).
  Design de colunas reconciliado com o armazenamento: ids `TEXT`,
  `provider_id`/`external_tx_id`/`round_id`/`game_id`/`idempotency_key`
  anuláveis (OPENING interno usa NULL), ledger imutável por trigger.

## Composição e ciclo de vida (Fx)

- Grafo: config → pool → migrations → portas → service → filas → workers →
  HTTP. `fx.Invoke(runMigrations, registerLifecycle)`.
- Workers e servidor iniciam no `OnStart`; `OnStop` cancela contextos e aguarda
  o término até `SHUTDOWN_TIMEOUT`. O serviço termina em SIGTERM/SIGINT.
- O teste `internal/app/composition_integration_test.go` valida a composição:
  start (migrations + health checks) e encerramento ordenado, com overrides
  (`fx.Replace`) apenas das dependências que exigiriam LocalStack/Keycloak.

## Observabilidade

- Logs estruturados JSON com `correlationId` propagado via middleware.
- Métricas (Prometheus): resultados de operações, duplicatas, retries da outbox
  e latência.
- `/health/live` (processo) e `/health/ready` (`Ping` no Postgres).

## Testes

- Unit: domínio, httpapi (handlers com stub), derr, wagering/wallet.
- Integração (`//go:build integration`, exigem Postgres local): schema
  constraints, fluxo completo open/process/replay/reject/reconcile,
  PENDING_REFERENCE resolvida pelo worker, e composição Fx start/stop.
  Rodados serializados (`-p 1`) porque resetam o mesmo banco:
  `go test -tags integration -count=1 -p 1 ./internal/...`
- E2E (`make up` + `make e2e`): LocalStack + Keycloak habilitam a malha
  completa HTTP+SQS+outbox.