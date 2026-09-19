#!/usr/bin/env bash
# Provisiona (idempotente) as filas SQS no LocalStack.
# Uso: AWS_ENDPOINT_URL=http://localhost:4566 bash deploy/localstack/provision-sqs.sh
set -euo pipefail

ENDPOINT="${AWS_ENDPOINT_URL:-http://localhost:4566}"
REGION="${AWS_REGION:-us-east-1}"

AWS_ARGS=(--endpoint-url "${ENDPOINT}" --region "${REGION}")
aws_cli() { aws "${AWS_ARGS[@]}" "$@"; }

# Cria a fila de entrada com redrive para a DLQ (FIFO).
ensure_fifo() {
  local name="$1"
  shift
  if aws_cli sqs get-queue-url --queue-name "${name}" >/dev/null 2>&1; then
    aws_cli sqs get-queue-url --queue-name "${name}"
    return
  fi
  aws_cli sqs create-queue \
    --queue-name "${name}" \
    --attributes "FifoQueue=true,ContentBasedDeduplication=true" "$@"
}

# 1) DLQ primeiro (o redrive referencia a ARN da DLQ).
DLQ_URL="$(ensure_fifo "wager-transactions-dlq.fifo")"
DLQ_ARN="$(aws_cli sqs get-queue-attributes \
  --queue-url "${DLQ_URL}" \
  --attribute-names QueueArn \
  --query 'Attributes.QueueArn' --output text)"

DLQ_POLICY="$(printf '{"deadLetterTargetArn":"%s","maxReceiveCount":"5"}' "${DLQ_ARN}")"

# 2) Fila de entrada com redrive.
INPUT_URL="$(ensure_fifo "wager-transactions.fifo" "RedrivePolicy=${DLQ_POLICY}")"
echo "input queue: ${INPUT_URL}"

# 3) Fila de eventos da transactional outbox.
EVENT_URL="$(ensure_fifo "wallet-events.fifo")"
echo "event queue: ${EVENT_URL}"

echo "SQS provisionado em ${ENDPOINT}"