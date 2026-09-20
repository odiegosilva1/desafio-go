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

## Mensageria — wire e convenções FIFO

- **Wire (entrada)**: o corpo é o envelope do §10 do specs
  (`messageId`, `type`, `occurredAt`, `data`). O tipo `InboundEnvelope`
  compartilhado em `internal/messaging` define o contrato serializado usado pelo
  consumidor (`ParseEnvelope`), testado compatível com o exemplo do specs.
- **`MessageGroupId` da entrada = `data.walletId`**: preserva a ordem por
  carteira (importante para o lock serializado por carteira).
- **`MessageDeduplicationId` da entrada = `messageId`** do envelope; a janela de
  deduplicação de 5 min do SQS cobre republicações na entrada.
- **Concorrência HTTP × SQS**: entradas HTTP e SQS compartilham o mesmo caso de
  uso e as mesmas chaves únicas; a primeira que comitar define o efeito, a
  segunda é deduplicada (idempotência persistente).

### Consumidor SQS — tentativas, visibility e mensagens inválidas

- **Visibility timeout**: `30s` (`messaging.VisibilityTimeout`), aplicado no
  `ReceiveMessage`. Uma falha transitória durante o tratamento deixa a mensagem
  na fila; ao expirar o prazo, o broker a disponibiliza novamente e o próximo
  recebimento é a forma de retry. Em `SIGTERM` o loop de polling para e o
  tratamento em andamento é concluído dentro do prazo; sem isso, a visibilidade
  expira e a reentrega é segura.
- **Backoff exponencial nas falhas transitórias**: em vez de deixar a redelivery
  imediata ao fim do `VisibilityTimeout`, o consumidor **estende a visibilidade**
  (`ChangeMessageVisibility`) para `5s * 2^(n-1)` (teto `600s`), lendo o
  `ApproximateReceiveCount` do broker. Isso evita `thundering-herd` entre as
  instâncias e dá ao destino tempo para se recuperar. Se a extensão falhar, a
  reentrega ocorre pelo prazo padrão.
- **Identidade de inbox = `messageId` do envelope**: a duplicação é ancorada no
  identificador de negócio estável da mensagem (§10), não no `MessageId` nativo
  do SQS (que pode variar em reentregas). O registro da inbox e o tratamento
  compartilham o mesmo commit; a remoção da fila acontece **somente depois**. Um
  processo que morre entre o commit e o delete reentrega a mesma mensagem,
  deduplicada pela inbox (nunca um segundo efeito financeiro). O `INSERT` da
  inbox usa `ON CONFLICT DO NOTHING` para não abortar a transação (25P02) na
  reentrega.
- **Limite de tentativas**: o redrive da entrada aponta para a DLQ com
  `maxReceiveCount = 5` (`deploy/localstack/provision-sqs.sh`); exauridas as
  tentativas, o **próprio SQS** move a mensagem à DLQ. Falhas inequivocamente
  permanentes são encaminhadas à DLQ **imediatamente** pelo consumidor, sem
  esgotar as tentativas.
- **Mensagens inválidas**: corpo não parseável → `INVALID_MESSAGE`; envelope
  válido, porém fora das regras de domínio (entrada inválida) → codifica o
  `failureCode` (ex.: `INVALID_MONEY`); reentrega com conteúdo diferente do hash
  original → `PAYLOAD_MISMATCH`. Todas vão à DLQ e são removidas da fila, sem
  efeito no banco.
- **Rejeições de negócio** (`ClassBusinessRule`) são **terminais**: confirmadas
  na inbox com o `FailureCode`, mensagem removida, **sem** DLQ (specs §10).
- **Outbox com backoff durável**: falhas de publicação reagem no próprio registro
  (`attempts++` e `next_attempt_at` com backoff exponencial persistido — `1s *
  2^n`, teto `5min`), compartilhado entre instâncias; o lote continua para não
  deixar uma mensagem problemática bloquear as demais, e `MarkPublished` com o
  mesmo `eventId` garante idempotência na republicação.

## Reversões e PENDING_REFERENCE

- **REFUND** sobre BET e **ROLLBACK** sobre BET/WIN/REFUND (qualquer outra
  combinação é `REVERSAL_MISMATCH`), com validação de
  provedor/jogador/carteira/moeda/rodada/valor.
- **Reversão duplicada por efeito financeiro (§7)**: uma referência nunca recebe
  duas reversões **processadas** com o mesmo efeito — REFUND e ROLLBACK de uma
  BET ambas devolvem o mesmo débito (`CREDIT`); dois ROLLBACK de uma WIN/REFUND
  debitam o mesmo crédito (`DEBIT`). A segunda é `DUPLICATE_REVERSAL`. Isso
  impede a devolução duplicada do mesmo débito (Refund+Rollback sobre a mesma
  BET) e a reversão em cascata do mesmo crédito, sem depender de ordem ou
  convenção de nomenclatura.
- Operação que referencia uma transação ainda inexistente/em andamento não
  falha: entra em **PENDING_REFERENCE** com `next_attempt_at` em backoff
  exponencial e é retomada pelo `ReferenceWorker` (múltiplas instâncias
  disputam com `FOR UPDATE SKIP LOCKED`).
- **TTL**: `MaxReferenceAttempts = 5`; esgotado → `REJECTED`
  (`REFERENCE_NOT_FOUND`), sem efeito financeiro.
- **Moeda**: a moeda de cada operação (incluindo LOSS) deve coincidir com a da
  carteira (`CURRENCY_MISMATCH`); o schema vigora BRL-only e já rejeita na
  escrita — a checagem de domínio é a rede de segurança para multi-moeda.

## Autenticação e autorização

- OAuth 2.0/OIDC `client_credentials` no Keycloak. O `client_id` do JWT define
  o **providerId autenticado**: o provedor só enxerga suas próprias carteiras
  e transações (isolamento 404).
- Cliente interno (`wallet-service-internal`) e escopos
  `wallet:provider` / `wallet:internal` / `wallet:admin` habilitam
  abertura/reconciliação internas; `X-Wallet-Internal-Api-Key` protege a via
  interna (não pública).
- Health checks públicos. Métricas Prometheus em `/metrics`.

## Contrato HTTP — códigos de status e corpos

Situações distintas são distinguíveis pelo status e por um `failureCode`
estável respondido no corpo (`{"code":"...","message":"..."}`):

| Status | Situação | `failureCode` (exemplos) |
| --- | --- | --- |
| `200 OK` | Operação concluída (`idempotentReplay` `false`/`true`), leituras de carteira/ledger/transação, reconciliação consistente | — |
| `201 Created` | Carteira aberta (`POST /wallets`) | — |
| `202 Accepted` | Operação aguardando referência (`PENDING_REFERENCE`) | `PENDING_REFERENCE` |
| `400 Bad Request` | Entrada inválida: JSON corrupto/campos desconhecidos, `Money` inválido, `Idempotency-Key` ausente, cursor/limit inválidos, `OPENING` por HTTP/SQS | `INVALID_PAYLOAD`, `INVALID_MONEY`, `MISSING_IDEMPOTENCY_KEY`, `INVALID_CURSOR`, `OPENING_BLOCKED` |
| `404 Not Found` | Carteira/transação inexistente ou **fora do provedor autenticado** (isolamento §2) | `NOT_FOUND`, `WALLET_NOT_FOUND` |
| `409 Conflict` | Chave de idempotência reutilizada com payload diferente; abertura duplicada para o mesmo `(playerId, currency)` | `IDEMPOTENCY_CONFLICT`, `DUPLICATE_WALLET` |
| `422 Unprocessable Entity` | Rejeição de negócio definitiva (terminal, sem efeito financeiro) | `INSUFFICIENT_FUNDS`, `REVERSAL_INSUFFICIENT_FUNDS`, `DUPLICATE_REVERSAL`, `REVERSAL_MISMATCH`, `ZERO_VALUE_*`, moeda incompatível |
| `503 Service Unavailable` | Falha transitória/indisponibilidade de dependência (retry do cliente) | `TRANSIENT`, `CONCURRENT_UPDATE` |

Respostas de erro usam o corpo `{"code": "<failureCode>", "message": "..."}` e são
distinguíveis por classe de domínio (`ClassInvalidInput` → 400,
`ClassBusinessRule` → 422, `ClassConflict` → 409, `ClassPendingReference` → 202,
transitórias → 503).

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

- Unit: domínio, httpapi (handlers com stub), derr, wagering/wallet, app
  (reversões — `validateReversal`/`reversalEffect`, zero-policy), messaging
  (backoff exponencial do consumidor).
- Integração (`//go:build integration`, exigem Postgres local): schema
  constraints, fluxo completo open/process/replay/reject/reconcile,
  PENDING_REFERENCE resolvida pelo worker, composição Fx start/stop, disputa
  obrigatória 80.00×80.00 sobre 100.00, reentrega pós-crash, expiração de
  referência, conflito de idempotência e eventos do OPENING.
  Rodados serializados (`-p 1`) porque resetam o mesmo banco:
  `go test -tags integration -count=1 -p 1 ./internal/... ./test/...`
- Múltiplas instâncias (`test/multiinstance`) e simulações de falha
  (`test/faults`): ver README §Testes de integração. Rodam com o mesmo Postgres.
- **Malha real** (`test/realstack`, `make test-realstack`): a aplicação de
  produção é iniciada contra PostgreSQL + LocalStack (SQS) + Keycloak (OIDC)
  **reais**, sem fakes — valida autenticação efetiva, consumidor SQS com inbox,
  transactional outbox no destino e reconciliação (§2/§10/§11). Sem a infra,
  o teste escreve SKIP.
- E2E (`make up` + `make e2e`): LocalStack + Keycloak habilitam a malha
  completa HTTP+SQS+outbox.

## Limitações, interpretações e trabalho não concluído

- **Contrato da DLQ (interpretação do §10)**: apenas **falha permanente**,
  **payload inválido** ou tentativas esgotadas vão à DLQ. Rejeições definitivas
  de negócio (saldo insuficiente, carteira inexistente, reversão duplicada) são
**terminais**: confirmadas na inbox com o `FailureCode` e a mensagem é
   removida da fila sem efeito financeiro. Validado em `test/faults`.
- **Autorização por identidade do cliente, não por escopo**: o `client_id` do
  JWT é o `providerId` (decisão do §2). Os escopos `wallet:provider`,
  `wallet:internal` e `wallet:admin` são definidos e emitidos pelo IdP, mas o
  ponto de autorização na aplicação é a **identidade do cliente** (e o
  isolamento por provedor no domínio); `auth.Principal.HasScope` não é cobrado
  no middleware. O cliente interno é identificado pelo `client_id`.
- **Usuário humano é demonstração**: o realm importa `tester` para fluxos
  interativos (password grant) via `wallet-service`. Em produção **todo** acesso
  ao negócio é `client_credentials` de um cliente-provedor; nenhum usuário
  humano autentica na API.
- **Idempotência da inbox por SQS MessageId**: reentregas do mesmo envio são
  deduplicadas pela inbox. A deduplicação financeira é por
  `(providerId, idempotencyKey)`, o que torna inofensiva uma reexecução com
  `MessageId` novo — os dois níveis somados garantem exactly-once.
- **Janela de republicação da outbox**: a publicação acontece **antes** do
  commit do `MarkPublished`. Uma falha entre publicar e confirmar reformata o
  registro PENDING e republica — seguro porque o `eventId` é estável e o destino
  usa `MessageDeduplicationId = eventId`. Impacto prático: conteúdo duplicado na
  entrada do destino durante a janela, nunca um evento novo com id novo.
- **`wallet-events.fifo` é solicitado apenas (publish)**: não há consumer de
  eventos nem replay de eventos; a reconciliação a montante fica com os clients.
- **Escala horizontal sem particionamento** das filas FIFO: a concorrência é
  resolvida no banco (`FOR UPDATE SKIP LOCKED` em carteira/outbox/referências);
  a regra FIFO por `walletId` preserva a ordem por carteira.
- **Não executado neste ambiente**: `docker compose up` (LocalStack, Keycloak)
  depende de Docker, indisponível no ambiente de desenvolvimento (permissão em
  `/var/run/docker.sock`). A malha HTTP+SQS+outbox foi validada por integração
  com fake SQS/OIDC contra o Postgres real.
- **Trabalho não concluído (roadmap)**: observabilidade refinada (traços,
  dashboards) e **CI/CD + reconciliação **periódica** (a reconciliação
  **pontual** por carteira existe: `POST /wallets/{id}/reconciliation`).
- **`make check` cobre os comandos §15**: `go vet ./...`, `go test ./...` e
  `go test -race ./...`; `make up` equivale a `docker compose up --build`.