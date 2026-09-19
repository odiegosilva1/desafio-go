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
cmd/migrate/              CLI de migrations (up|down)
cmd/producer/             injeta transações em wager-transactions.fifo
cmd/replay/               inspeciona e recupera a DLQ
internal/domain/          domínio puro (money, ledger, wagering, wallet, event, derr)
internal/application/     casos de uso, referências, outbox e reconciliação
internal/storage/         port (interfaces) + postgres (pgx, migrações)
internal/httpapi/         contratos HTTP, middlewares de auth e handlers
internal/auth/            validação OIDC e autorização por provedor
internal/messaging/       consumidor SQS + inbox, publisher, envelopes e provisionamento
internal/app/             módulos Fx, ciclo de vida e migrations embutidas
internal/observability/   logs JSON, métricas e health checks
deploy/                   realm Keycloak, filas SQS e cenários E2E
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
make migrate            # aplica migrations (up, default)
make migrate ARGS=down  # reverte migrations (até a versão anterior)
make test-integration   # integração (exige Postgres; usa -p 1)
make integration-check  # db-up + integração + race + db-down
make producer ARGS='...'# enfileira mensagens em wager-transactions.fifo
make replay ARGS='...'  # scan/requeue da DLQ
make e2e-scenario       # rejeição terminal + payload inválido→DLQ→replay
make e2e                # up + run (malha completa HTTP+SQS+Keycloak)
make down               # derruba a infra
```

## Ferramentas de operação (SQS)

### `cmd/producer` — injetar transações em `wager-transactions.fifo`

Produz mensagens com o mesmo wire do consumidor (`internal/messaging`). O
envelope é o JSON definido no §10 do `specs.md`:

```sh
# Constrói e enfileira um BET
go run ./cmd/producer wager -wallet <walletId> -player <playerId> \
  -round <roundId> -game <gameId> -kind BET -amount 25.00

# Envia envelopes prontos (objeto ou lista JSON), da stdin ou de arquivo
go run ./cmd/producer send -file envelope.json

# Imprime um envelope de exemplo
go run ./cmd/producer sample
```

Convenções FIFO (documentadas também em `ARCHITECTURE.md` §mensageria):
- `MessageGroupId = data.walletId` — preserva a ordem por carteira.
- `MessageDeduplicationId = messageId` do envelope (estável na janela de 5 min).
- `-repeat N` regenera o `messageId` por cópia (exige-o vazio no arquivo), para
  cenários de concorrência na mesma carteira.

### `cmd/replay` — DLQ `wager-transactions-dlq.fifo`

```sh
go run ./cmd/replay scan -limit 10        # inspeciona sem destruir
go run ./cmd/replay requeue -limit 10 -delete  # reenvia e remove da DLQ
```

Cada reenvio usa `MessageDeduplicationId` **novo** (UUID); reexecuções são
inofensivas, pois o serviço deduplica financeiramente por
`(providerId, idempotencyKey)` e pela inbox. O corpo é reenviado verbatim; o
`FailureCode` original é preservado como atributo informativo.

## Fluxos autenticados (Keycloak) e exemplos de chamadas

Com `make infra` o realm `wallet` é provisionado automaticamente com as
**identidades de teste** abaixo (todas com valores locais, ver
`deploy/keycloak/realm-export.json`):

| Identidade            | Tipo        | Segredo/credencial        | Enxerga                    |
|-----------------------|-------------|---------------------------|----------------------------|
| `provider-a`          | cliente OAuth (provedor A) | `provider-a-secret` | só carteiras/transações de `provider-a` |
| `provider-b`          | cliente OAuth (provedor B) | `provider-b-secret` | só carteiras/transações de `provider-b` |
| `wallet-service-internal` | cliente interno  | `wallet-internal-secret` | abertura/reconciliação (não é provedor) |
| `tester` / `tester`   | usuário (password grant)  | senha `tester`      | fluxo interativo via `wallet-service`    |

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

Enviar uma aposta e consultar o resultado:

```sh
WALLET="<walletId>"
curl -s http://localhost:8080/wagering/transactions \
  -H "Authorization: Bearer $TOKEN_A" -H 'Content-Type: application/json' \
  -d "{\"externalTransactionId\":\"tx-1\",\"playerId\":\"player-1\",\"walletId\":\"$WALLET\",
        \"roundId\":\"round-1\",\"gameId\":\"fortune-chimp\",\"kind\":\"BET\",
        \"money\":{\"amount\":\"25.00\",\"currency\":\"BRL\"}}"
# {"transactionId":"...","status":"PROCESSED","balance":{"amount":"75.00","currency":"BRL"},...}

curl -s http://localhost:8080/wallets/$WALLET \
  -H "Authorization: Bearer $TOKEN_A"
curl -s http://localhost:8080/wallets/$WALLET/ledger \
  -H "Authorization: Bearer $TOKEN_A"
```

Isolamento por provedor (o token de `provider-b` **não** enxerga a carteira de
`provider-a` → `404`), reconciliação apenas com o cliente interno e fluxo
interativo com o usuário de teste:

```sh
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
go test -tags integration -count=1 -p 1 ./internal/app/... ./internal/application/ ./internal/storage/... ./test/...
```

### Cenário multi-instância (`test/multiinstance`)

`TestMultiInstanceIndependenceAndRecovery` demonstra as garantias de escala
horizontal do specs (linhas 200/415): três **processos independentes**
(os/exec do próprio binário de teste, cada um com pool de conexões, outbox
publisher, reference worker e memória próprios) disputando o mesmo banco
apenas por `FOR UPDATE SKIP LOCKED`. Valida carteiras paralelas, serialização
da mesma carteira sob concorrência forte, rejeição por saldo, publicação da
outbox exatamente uma vez por eventId no agregado das instâncias, e retomada
de trabalho abandonado (`PENDING_REFERENCE` + outbox) após a morte brutal de
uma instância.

### Simulações de falha (`test/faults`)

Usam o **Postgres real** e um `fakeSQS` que implementa a interface
`messaging.SQSClient` (sem LocalStack), dirigindo o `Consumer` e o
`OutboxPublisher` de produção contra falhas:

- `TestConsumerDBOutageKeepsMessageAndRecovers` — banco indisponível durante o
  processamento: a mensagem **permanece** na fila (sem delete, sem DLQ) e, com o
  banco recuperado, a reentrega da mesma mensagem processa exatamente-uma-vez
  (inbox deduplica).
- `TestConsumerBusinessRejectionIsTerminalAndNoDLQ` — rejeição definitiva de
  negócio (saldo insuficiente) é **terminal**: consome a mensagem, registra na
  inbox com o código e **não** encaminha à DLQ (specs §10).
- `TestConsumerInvalidPayloadToDLQ` — corpo ilegível vai à DLQ com
  `INVALID_MESSAGE` e é removido da fila, sem efeito no banco.
- `TestOutboxPublisherRetryPublishesExactlyOnce` — destino de eventos fora do
  ar: registros permanecem `PENDING` (rollback, nenhum `MarkPublished` com
  falha) e, recuperado, cada eventId é entregue com id estável (dedup no
  destino) e nenhuma republicação após a confirmação.
- `TestOutboxPublisherSurvivesInstanceChange` — outra instância retoma registros
  herdados sem republicar os já confirmados (`published_by` por instância).

Contrato de DLQ (validado nos testes e no `deploy/e2e/scenario.sh`): apenas
**falha permanente**, **payload inválido** ou tentativas esgotadas vão à DLQ;
rejeições de negócio são confirmadas no inbox e removidas da fila.