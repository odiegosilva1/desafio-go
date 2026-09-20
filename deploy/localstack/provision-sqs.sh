#!/usr/bin/env bash
# Provisiona (idempotente) as filas SQS no LocalStack.
# Uso: AWS_ENDPOINT_URL=http://localhost:4566 bash deploy/localstack/provision-sqs.sh
set -euo pipefail

ENDPOINT="${AWS_ENDPOINT_URL:-http://localhost:4566}"
REGION="${AWS_REGION:-us-east-1}"

AWS_ARGS=(--endpoint-url "${ENDPOINT}" --region "${REGION}")
aws_cli() { aws "${AWS_ARGS[@]}" "$@"; }

# Cria uma fila FIFO (idempotente) e imprime a QueueUrl.
ensure_fifo() {
  local name="$1"
  if aws_cli sqs get-queue-url --queue-name "${name}" >/dev/null 2>&1; then
    aws_cli sqs get-queue-url --queue-name "${name}" --query 'QueueUrl' --output text
    return
  fi
  aws_cli sqs create-queue \
    --queue-name "${name}" \
    --attributes 'FifoQueue=true,ContentBasedDeduplication=true' \
    --query 'QueueUrl' --output text
}

# Aplica um atributo com valor JSON (ex.: RedrivePolicy) à fila.
set_json_attribute() {
  local queue_url="$1"
  local key="$2"
  local value="$3"
  local attrs
  attrs="$(python3 -c 'import json,sys; print(json.dumps({sys.argv[1]: sys.argv[2]}))' "${key}" "${value}")"
  aws_cli sqs set-queue-attributes --queue-url "${queue_url}" --attributes "${attrs}"
}

# 1) DLQ primeiro (o redrive referencia a ARN da DLQ).
DLQ_URL="$(ensure_fifo "wager-transactions-dlq.fifo")"
DLQ_ARN="$(aws_cli sqs get-queue-attributes \
  --queue-url "${DLQ_URL}" \
  --attribute-names QueueArn \
  --query 'Attributes.QueueArn' --output text)"

DLQ_POLICY="$(printf '{"deadLetterTargetArn":"%s","maxReceiveCount":"5"}' "${DLQ_ARN}")"

# 2) Fila de entrada com redrive para a DLQ.
INPUT_URL="$(ensure_fifo "wager-transactions.fifo")"
set_json_attribute "${INPUT_URL}" "RedrivePolicy" "${DLQ_POLICY}"
echo "input queue: ${INPUT_URL}"

# 3) Fila de eventos da transactional outbox.
EVENT_URL="$(ensure_fifo "wallet-events.fifo")"
echo "event queue: ${EVENT_URL}"

echo "SQS provisionado em ${ENDPOINT}"