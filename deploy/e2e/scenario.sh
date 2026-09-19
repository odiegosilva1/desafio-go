#!/usr/bin/env bash
# Cenário E2E do pipeline SQS (requer docker / LocalStack).
#
# Ciclo exercitado:
#   producer  →  wager-transactions.fifo  →  consumidor SQS  →  inbox FAILED
#              →  DLQ (WALLET_NOT_FOUND)  →  replay scan/requeue -delete  →  DLQ
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
DLQ="${AWS_SQS_DLQ:-wager-transactions-dlq.fifo}"
HTTP_ADDR="${HTTP_ADDR:-:8080}"
POLL_LIMIT="${E2E_POLL_LIMIT:-60}"   # segundos por espera
RUN="$RANDOM-$(date +%s)"
export AWS_ENDPOINT_URL AWS_REGION

say()  { printf '\033[1;34m[e2e]\033[0m %s\n' "$*"; }
ok()   { printf '\033[1;32m[PASS]\033[0m %s\n' "$*"; }
fail() { printf '\033[1;31m[FAIL]\033[0m %s\n' "$*" >&2; exit 1; }

command -v jq >/dev/null || fail "jq é obrigatório"
command -v docker >/dev/null || fail "docker é obrigatório"

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

# --- 3. Aposta em carteira inexistente → DLQ (WALLET_NOT_FOUND) -------------
EXT="e2e-$RUN"
say "enfileirando BET da carteira inexistente e2e-wallet-$RUN..."
"$TMP/producer" wager \
  -provider provider-a -ext-id "$EXT" \
  -wallet "e2e-wallet-$RUN" -player "e2e-player-$RUN" \
  -round "e2e-round-$RUN" -game "fortune-chimp" \
  -kind BET -amount "25.00" -currency BRL -log-level error

dlq_count() {
  "$TMP/replay" scan -limit 10 -log-level error 2>/dev/null \
    | sed -n 's/^mensagens na DLQ: //p' || true
}

say "aguardando entrada na DLQ..."
for i in $(seq 1 "$POLL_LIMIT"); do
  [ "$(dlq_count)" -ge 1 ] 2>/dev/null && break
  [ "$i" = "$POLL_LIMIT" ] && fail "mensagem não chegou à DLQ (consumidor ok? 'make up')"
  sleep 1
done

say "DLQ contém a rejeição; inspecionando (scan)..."
OUT="$("$TMP/replay" scan -limit 10 -log-level info 2>/dev/null)"
printf '%s\n' "$OUT"
echo "$OUT" | grep -q 'WALLET_NOT_FOUND' \
  || fail "FailureCode esperado WALLET_NOT_FOUND ausente no scan"
ok "consumidor rejeitou e encaminhou à DLQ com WALLET_NOT_FOUND"

# --- 4. Replay: reenvia e remove da DLQ -------------------------------------
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

# --- 5. Inbox durável (opcional) --------------------------------------------
if docker compose ps 2>/dev/null | grep -q postgres; then
  say "verificando inbox (FAILED/WALLET_NOT_FOUND)..."
  ROWS="$(docker compose exec -T postgres psql -U wallet -d wallet -tAc \
    "SELECT count(*) FROM inbox WHERE consumer_name='sqs-wager-transactions' AND status='FAILED';" 2>/dev/null || echo 0)"
  if [ "${ROWS:-0}" -ge 1 ]; then
    ok "inbox registrou rejeição durável (FAILED) — $ROWS registros"
  else
    say "aviso: nenhum registro FAILED na inbox (consulte o banco para diagnóstico)"
  fi
else
  say "aviso: postgres não localizado para inspeção da inbox (pule)"
fi

ok "cenário E2E concluído (ext ids com sufixo $RUN)"