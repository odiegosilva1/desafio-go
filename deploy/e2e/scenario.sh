#!/usr/bin/env bash
# Cenário E2E do pipeline SQS (requer docker / LocalStack).
#
# Ciclo exercitado:
#   1. Rejeição definitiva de negócio (carteira inexistente) é TERMINAL:
#      consumidor registra na inbox (PROCESSED/código) e NÃO encaminha à DLQ
#      (specs §10 — apenas falha permanente, payload inválido ou tentativas
#      esgotadas vão à DLQ).
#   2. Payload inválido → DLQ com INVALID_MESSAGE + remoção da fila.
#   3. replay scan/requeue -delete: DLQ → entrada → DLQ (circuito fechado).
#
# Pré-requisitos:
#   - `make infra` (Postgres + LocalStack + Keycloak) com a aplicação no ar
#     (`make up`, ou servidor local com `make run`).
#   - aws cli configurado p/ o endpoint (ou awslocal) e jq.
# Uso:
#   make e2e-scenario        (usa as variáveis do ambiente)
set -euo pipefail

: "${AWS_ENDPOINT_URL:?exporte AWS_ENDPOINT_URL (ex.: http://localhost:4566)}"
: "${AWS_REGION:=us-east-1}"
INPUT_QUEUE="${AWS_SQS_QUEUE:-wager-transactions.fifo}"
DLQ_NAME="${AWS_SQS_DLQ:-wager-transactions-dlq.fifo}"
HTTP_ADDR="${HTTP_ADDR:-:8080}"
POLL_LIMIT="${E2E_POLL_LIMIT:-60}"   # segundos por espera
RUN="$RANDOM-$(date +%s)"
export AWS_ENDPOINT_URL AWS_REGION

say()  { printf '\033[1;34m[e2e]\033[0m %s\n' "$*"; }
ok()   { printf '\033[1;32m[PASS]\033[0m %s\n' "$*"; }
fail() { printf '\033[1;31m[FAIL]\033[0m %s\n' "$*" >&2; exit 1; }

command -v jq >/dev/null || fail "jq é obrigatório"
command -v docker >/dev/null || fail "docker é obrigatório"
command -v aws >/dev/null || fail "aws cli é obrigatório (use --endpoint-url ou awslocal)"

# --- 1. Pré-condições: app e LocalStack -------------------------------------
say "checando LocalStack ($AWS_ENDPOINT_URL)..."
for i in $(seq 1 "$POLL_LIMIT"); do
  if curl -sf --max-time 2 "$AWS_ENDPOINT_URL/_localstack/health" | grep -q '"sqs":"available"'; then
    break
  fi
  [ "$i" = "$POLL_LIMIT" ] && fail "LocalStack SQS indisponível; rode: make infra"
  sleep 1
done

say "checando a aplicação (liveness)..."
host="${HTTP_ADDR%:*}"; port="${HTTP_ADDR##*:}"
[ "$host" = "" ] && host="localhost"
if ! curl -sf --max-time 2 "http://$host:$port/health/live" >/dev/null; then
  fail "aplicação fora do ar em http://$host:$port (make up / make run)"
fi

# --- 2. Compila as ferramentas ----------------------------------------------
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
say "compilando producer e replay..."
go build -o "$TMP/producer" ./cmd/producer
go build -o "$TMP/replay" ./cmd/replay

INPUT_URL="$(aws sqs get-queue-url --queue-name "$INPUT_QUEUE" --query QueueUrl --output text)"

input_pending() {
  aws sqs get-queue-attributes --queue-url "$INPUT_URL" \
    --attribute-names ApproximateNumberOfMessages \
    --query 'Attributes.ApproximateNumberOfMessages' --output text 2>/dev/null || echo 0
}

dlq_count() {
  "$TMP/replay" scan -limit 100 -log-level error 2>/dev/null \
    | sed -n 's/^mensagens na DLQ: //p' || true
}

# --- 3. Rejeição de negócio é terminal (SEM DLQ) ----------------------------
say "assegurando DLQ vazia no início..."
awk '{n+=$1} END {exit !(n==0)}' <<< "$(dlq_count)" || fail "DLQ não está vazia; limpe/redrive antes de rodar"

EXT="e2e-$RUN"
say "enfileirando BET de carteira inexistente e2e-wallet-$RUN (rejeição de negócio)..."
"$TMP/producer" wager \
  -provider provider-a -ext-id "$EXT" \
  -wallet "e2e-wallet-$RUN" -player "e2e-player-$RUN" \
  -round "e2e-round-$RUN" -game "fortune-chimp" \
  -kind BET -amount "25.00" -currency BRL -log-level error

say "aguardando consumo da mensagem (fila de entrada esvaziando)..."
for i in $(seq 1 "$POLL_LIMIT"); do
  [ "$(input_pending)" = "0" ] 2>/dev/null && break
  [ "$i" = "$POLL_LIMIT" ] && fail "producer BET não foi consumido (consumidor ok? 'make up')"
  sleep 1
done
say "verificando que a rejeição NÃO foi à DLQ (specs §10)..."
if [ "$(dlq_count)" -ge 1 ] 2>/dev/null; then
  fail "rejeição de negócio NÃO deve ir à DLQ (specs §10)"
fi
ok "rejeição de negócio consumida como terminal (sem DLQ)"

# --- 4. Payload inválido → DLQ (INVALID_MESSAGE) ----------------------------
say "enfileirando payload inválido com deduplicação própria..."
aws sqs send-message \
  --queue-url "$INPUT_URL" \
  --message-body '{"bad":1' \
  --message-group-id "e2e-invalid-$RUN" \
  --message-deduplication-id "e2e-invalid-$RUN" >/dev/null

say "aguardando entrada na DLQ com INVALID_MESSAGE..."
for i in $(seq 1 "$POLL_LIMIT"); do
  [ "$(dlq_count)" -ge 1 ] 2>/dev/null && break
  [ "$i" = "$POLL_LIMIT" ] && fail "payload inválido não chegou à DLQ (consumidor ok? 'make up')"
  sleep 1
done
OUT="$("$TMP/replay" scan -limit 10 -log-level info 2>/dev/null)"
printf '%s\n' "$OUT"
echo "$OUT" | grep -q 'INVALID_MESSAGE' \
  || fail "FailureCode esperado INVALID_MESSAGE ausente no scan"
ok "payload inválido encaminhado à DLQ com INVALID_MESSAGE"

# --- 5. Replay: reenvia e remove da DLQ -------------------------------------
say "replay requeue -delete..."
"$TMP/replay" requeue -limit 10 -delete -log-level error
# A mensagem reenviada volta a ser processada → DLQ novamente (circuito fechado).
say "aguardando retorno à DLQ após o replay..."
for i in $(seq 1 "$POLL_LIMIT"); do
  [ "$(dlq_count)" -ge 1 ] 2>/dev/null && break
  [ "$i" = "$POLL_LIMIT" ] && fail "replay não foi reprocessado (volta à DLQ)"
  sleep 1
done
ok "replay reprocessado: DLQ → entrada → DLQ concluído"

# --- 6. Inbox durável (opcional) --------------------------------------------
if docker compose ps 2>/dev/null | grep -q postgres; then
  say "verificando inbox (rejeição de negócio PROCESSED/WALLET_NOT_FOUND)..."
  ROWS="$(docker compose exec -T postgres psql -U wallet -d wallet -tAc \
    "SELECT count(*) FROM inbox WHERE consumer_name='sqs-wager-transactions' AND status='PROCESSED' AND failure_code='WALLET_NOT_FOUND';" 2>/dev/null || echo 0)"
  if [ "${ROWS:-0}" -ge 1 ]; then
    ok "inbox registrou a rejeição durável (PROCESSED/WALLET_NOT_FOUND) — $ROWS registros"
  else
    say "aviso: nenhum registro PROCESSED/WALLET_NOT_FOUND na inbox (consulte o banco para diagnóstico)"
  fi
else
  say "aviso: postgres não localizado para inspeção da inbox (pule)"
fi

ok "cenário E2E concluído (ext ids com sufixo $RUN)"