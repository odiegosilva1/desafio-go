-- 0002_wagering.up.sql
-- Transações de aposta, inbox (consumidor) e outbox (publicador).
-- Inbox/outbox/outbox-dlq são tabelas; a integração SQS é tratada na camada
-- messaging (inbox→consumer persiste, outbox→publisher envia).

BEGIN;

-- ---------------------------------------------------------------------------
-- Transações de aposta (reidratação do agregado Transaction do wagering)
-- ---------------------------------------------------------------------------
CREATE TABLE wagering_transactions (
    id                      UUID         PRIMARY KEY,
    provider_id             UUID         NOT NULL,
    player_id               UUID         NOT NULL,
    wallet_id               UUID         NOT NULL REFERENCES wallets (id),
    round_id                UUID         NOT NULL,
    game_id                 UUID         NOT NULL,
    kind                    TEXT         NOT NULL,  -- BET/WIN/LOSS/REFUND/ROLLBACK
    amount_units            BIGINT       NOT NULL,
    currency                CHAR(3)      NOT NULL,
    external_tx_id          TEXT         NOT NULL,
    external_id             TEXT         NOT NULL,
    provider_ext_tx_id      TEXT         NOT NULL,
    reference_ext_id        TEXT         NULL,      -- referência de REFUND/ROLLBACK
    idempotency_key         TEXT         NOT NULL,
    correlation_id          UUID         NULL,
    causation_id            UUID         NULL,
    occurred_at             TIMESTAMPTZ  NOT NULL,
    recorded_at             TIMESTAMPTZ  NOT NULL,
    processed_at            TIMESTAMPTZ  NULL,
    wallet_version          BIGINT       NOT NULL DEFAULT 0,
    failure_code            TEXT         NULL,
    failure_message         TEXT         NULL,
    payload_hash            TEXT         NOT NULL,
    state                   TEXT         NOT NULL,  -- PENDING/PENDING_REFERENCE/PROCESSED/REJECTED/FAILED

    -- idempotência da transação original (nunca processa o mesmo evento 2x)
    CONSTRAINT wagering_tx_external_unique UNIQUE (provider_id, provider_ext_tx_id),

    CONSTRAINT wagering_tx_kind_valid CHECK (kind IN ('BET','WIN','LOSS','REFUND','ROLLBACK')),
    CONSTRAINT wagering_tx_state_valid CHECK (state IN ('PENDING','PENDING_REFERENCE','PROCESSED','REJECTED','FAILED')),
    CONSTRAINT wagering_tx_currency_positive CHECK (currency IN ('BRL'))
);

CREATE INDEX idx_wagering_wallet_state ON wagering_transactions (wallet_id, state);
CREATE INDEX idx_wagering_external ON wagering_transactions (provider_id, provider_ext_tx_id叙)

-- ---------------------------------------------------------------------------
-- Outbox (publicação durável, transação atômica com o evento de origem)
-- ---------------------------------------------------------------------------
CREATE TABLE outbox (
    id             UUID         PRIMARY KEY,
    aggregate_type TEXT         NOT NULL,
    aggregate_id   UUID         NOT NULL,
    event_type     TEXT         NOT NULL,      -- ex.: WagerTransactionProcessed
    payload        JSONB        NOT NULL,
    correlation_id UUID         NULL,
    causation_id   UUID         NULL,
    idempotency_key TEXT        NOT NULL,
    occurred_at    TIMESTAMPTZ  NOT NULL,      -- quando a operação originou o evento
    recorded_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
    attempts       INT          NOT NULL DEFAULT 0,
    next_attempt   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    status         TEXT         NOT NULL DEFAULT 'PENDING',  -- PENDING/PUBLISHED/DLQ
    published_at   TIMESTAMPTZ  NULL
);

CREATE INDEX idx_outbox_status ON outbox (status, next_attempt);
CREATE INDEX idx_outbox_correlation ON outbox (correlation_id);

-- ---------------------------------------------------------------------------
-- Inbox (consumidor durável: idempotência por mensagem SQS)
-- ---------------------------------------------------------------------------
CREATE TABLE inbox (
    id             UUID         PRIMARY KEY,
    consumer_name TEXT         NOT NULL,
    message_id     TEXT         NOT NULL,
    payload        JSONB        NOT NULL,
    received_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
    processed_at   TIMESTAMPTZ  NULL,
    status         TEXT         NOT NULL DEFAULT 'RECEIVED', -- RECEIVED/PROCESSED/FAILED
    failure_code   TEXT         NULL,

    CONSTRAINT inbox_consumer_msg_unique UNIQUE (consumer_name, message_id)
);

CREATE INDEX idx_inbox_status ON inbox (status, received_at);

COMMIT;
