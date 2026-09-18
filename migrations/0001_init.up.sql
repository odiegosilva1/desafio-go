-- 0001_init.up.sql
-- Domínio financeiro: carteira, ledger imutável e transações de aposta,
-- com outbox/inbox e persistência de idempotência.

BEGIN;

-- ---------------------------------------------------------------------------
-- Carteiras
-- ---------------------------------------------------------------------------
CREATE TABLE wallets (
    id            UUID PRIMARY KEY,
    provider_id   UUID        NOT NULL,
    player_id     UUID        NOT NULL,
    currency      CHAR(3)     NOT NULL,
    -- saldo corrente em unidades inteiras da moeda (centavos para BRL)
    balance_units BIGINT      NOT NULL DEFAULT 0,
    -- versão otimista/pessimista: incrementada a cada movimentação
    version       BIGINT      NOT NULL DEFAULT 0,
    status        TEXT        NOT NULL DEFAULT 'ACTIVE',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT wallets_balance_nonnegative CHECK (balance_units >= 0),

    -- Uma carteira pertence a um provedor+player+moeda (isolamento por provedor)
    CONSTRAINT wallets_unique_provider_player_currency
        UNIQUE (provider_id, player_id, currency)
);

CREATE INDEX idx_wallets_player ON wallets (player_id);
CREATE INDEX idx_wallets_provider ON wallets (provider_id);

-- ---------------------------------------------------------------------------
-- Ledger imutável (append-only)
-- ---------------------------------------------------------------------------
-- Cada linha é um lançamento financeiro. NUNCA é atualizada nem removida:
-- toda movimentação gera DEBIT/CREDIT com saldo anterior/posterior e versão.
CREATE TABLE wallet_ledger (
    id             UUID PRIMARY KEY,
    wallet_id      UUID         NOT NULL REFERENCES wallets (id),
    tx_type        TEXT         NOT NULL,                -- BET/WIN/LOSS/REFUND/ROLLBACK/OPENING
    direction      TEXT         NOT NULL,                -- DEBIT/CREDIT
    amount_units   BIGINT       NOT NULL,
    balance_before BIGINT       NOT NULL,
    balance_after  BIGINT       NOT NULL,
    version        BIGINT       NOT NULL,
    external_tx_id TEXT         NOT NULL,                -- idempotência do provedor
    provider_id    UUID         NOT NULL,
    player_id      UUID         NOT NULL,
    round_id       UUID         NOT NULL,
    game_id        UUID         NOT NULL,
    reference_tx   UUID         NULL,                    -- referência (REFUND/ROLLBACK)
    correlation_id UUID         NULL,
    causation_id   UUID         NULL,
    occurred_at    TIMESTAMPTZ  NOT NULL,
    recorded_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),

    -- Imutabilidade: composição única garante que cada (wallet, versão) seja
    -- emitida uma única vez. Replay/provisor de referência não duplica.
    CONSTRAINT ledger_immutable UNIQUE (wallet_id, version, external_tx_id, direction, amount_units),

    -- Continudade de saldo por carteira (evita saltos/corrupção do ledger)
    CONSTRAINT ledger_balance_match
        CHECK (balance_after = balance_before + CASE WHEN direction = 'CREDIT' THEN amount_units ELSE -amount_units END),
    CONSTRAINT ledger_amount_positive CHECK (amount_units >= 0),
    CONSTRAINT ledger_direction_valid CHECK (direction IN ('DEBIT', 'CREDIT'))
);

CREATE INDEX idx_ledger_wallet_version ON wallet_ledger (wallet_id, version);
CREATE INDEX idx_ledger_external_tx ON wallet_ledger (external_tx_id);
CREATE INDEX idx_ledger_reference_tx ON wallet_ledger (reference_tx);

-- ---------------------------------------------------------------------------
-- Transações de aposta (máquina de estados persistida)
-- ---------------------------------------------------------------------------
CREATE TABLE wagering_transactions (
    id                 UUID PRIMARY KEY,
    external_tx_id     TEXT         NOT NULL,
    external_tx_type   TEXT         NOT NULL,            -- BET/WIN/LOSS/REFUND/ROLLBACK
    provider_id        UUID         NOT NULL,
    player_id          UUID         NOT NULL,
    wallet_id          UUID         NOT NULL REFERENCES wallets (id),
    round_id           UUID         NOT NULL,
    game_id            UUID         NOT NULL,
    kind               TEXT         NOT NULL,
    amount_units       BIGINT       NOT NULL DEFAULT 0,
    currency           CHAR(3)      NOT NULL,
    reference_ext_tx   TEXT         NULL,
    idempotency_key    TEXT         NOT NULL,
    correlation_id     UUID         NULL,
    causation_id       UUID         NULL,
    occurred_at        TIMESTAMPTZ  NOT NULL,
    recorded_at        TIMESTAMPTZ  NOT NULL DEFAULT now(),
    processed_at       TIMESTAMPTZ  NULL,
    processing_result  TEXT         NULL,                -- status da transação
    failure_code       TEXT         NULL,
    failure_message    TEXT         NULL,
    payload_hash       TEXT         NOT NULL,            -- hash canônico do snapshot
    state              TEXT         NOT NULL DEFAULT 'PENDING',
    wallet_version     BIGINT       NOT NULL DEFAULT 0,

    -- Idempotência por chave (Idempotency-Key persistida): nunca processa duas
    -- vezes a mesma chave; replay devolve o resultado original.
    CONSTRAINT wagering_idempotency_unique UNIQUE (idempotency_key),
    -- Um external_tx_id do provedor só aparece uma vez (evita reenvio como BET nova)
    CONSTRAINT wagering_external_unique UNIQUE (provider_id, external_tx_id)
);

CREATE INDEX idx_wagering_wallet_state ON wagering_transactions (wallet_id, state);
CREATE INDEX idx_wagering_reference_pending ON wagering_transactions (state, reference_ext_tx)
    WHERE reference_ext_tx IS NOT NULL;

-- ---------------------------------------------------------------------------
-- Outbox (publicação atômica com a operação de domínio)
-- ---------------------------------------------------------------------------
CREATE TABLE outbox (
    id             UUID PRIMARY KEY,
    aggregate_type TEXT         NOT NULL,                -- wallet / wagering
    aggregate_id   UUID         NOT NULL,
    event_type     TEXT         NOT NULL,                -- WagerTransactionProcessed etc.
    payload        JSONB        NOT NULL,
    correlation_id UUID         NULL,
    causation_id   UUID         NULL,
    idempotency_key TEXT        NOT NULL,                -- mesma chave da operação
    status         TEXT         NOT NULL DEFAULT 'PENDING',  -- PENDING/PUBLISHED/FAILED
    attempts       INT          NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    produced_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
    published_at   TIMESTAMPTZ  NULL,

    CONSTRAINT outbox_status_valid CHECK (status IN ('PENDING', 'PUBLISHED', 'FAILED'))
);

CREATE INDEX idx_outbox_pending ON outbox (status, next_attempt_at)
    WHERE status = 'PENDING';

-- ---------------------------------------------------------------------------
-- Inbox (idempotência do consumidor SQS: mensagem processada uma única vez)
-- ---------------------------------------------------------------------------
CREATE TABLE inbox (
    consumer_name TEXT        NOT NULL,
    message_id    TEXT        NOT NULL,
    payload_hash  TEXT        NOT NULL,
    status        TEXT        NOT NULL DEFAULT 'RECEIVED',  -- RECEIVED/PROCESSED/FAILED
    received_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    processed_at  TIMESTAMPTZ NULL,

    CONSTRAINT inbox_unique_message UNIQUE (consumer_name, message_id),
    CONSTRAINT inbox_status_valid CHECK (status IN ('RECEIVED', 'PROCESSED', 'FAILED'))
);

-- ---------------------------------------------------------------------------
-- Meta de migrations (aplicada pela ferramenta de migração embutida)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS schema_version (
    version    INT          NOT NULL,
    applied_at TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (version)
);

COMMIT;
