-- FinTrack v2 – схема БД. Деньги хранятся в МИНОРНЫХ единицах (копейки, BIGINT).

-- Счета
CREATE TABLE IF NOT EXISTS accounts (
    id          TEXT PRIMARY KEY,
    owner_name  TEXT NOT NULL,
    currency    TEXT NOT NULL DEFAULT 'RUB',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Двойная запись: каждый перевод = debit со счёта-источника + credit на счёт-получатель.
-- UNIQUE(payment_id, account_id, direction) даёт идемпотентность: повторная обработка
-- того же ledger.transfer.requested не задвоит проводки.
CREATE TABLE IF NOT EXISTS ledger_entries (
    id          BIGSERIAL PRIMARY KEY,
    payment_id  TEXT NOT NULL,
    account_id  TEXT NOT NULL REFERENCES accounts(id),
    direction   TEXT NOT NULL CHECK (direction IN ('debit', 'credit')),
    amount      BIGINT NOT NULL CHECK (amount > 0),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (payment_id, account_id, direction)
);
CREATE INDEX IF NOT EXISTS idx_ledger_account ON ledger_entries(account_id);

-- Баланс счёта = сумма credit минус сумма debit
CREATE OR REPLACE VIEW balances AS
SELECT a.id AS account_id,
       a.owner_name,
       COALESCE(SUM(CASE WHEN e.direction = 'credit' THEN e.amount ELSE -e.amount END), 0) AS balance
FROM accounts a
LEFT JOIN ledger_entries e ON e.account_id = a.id
GROUP BY a.id, a.owner_name;

-- Transactional outbox: ledger пишет проводки и событие в ОДНОЙ транзакции,
-- отдельный publisher relay-ит неопубликованные строки в Kafka.
CREATE TABLE IF NOT EXISTS outbox (
    id           BIGSERIAL PRIMARY KEY,
    topic        TEXT NOT NULL,
    msg_key      TEXT,
    payload      JSONB NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_outbox_unpublished ON outbox(id) WHERE published_at IS NULL;

-- Идемпотентность на входе (gateway): один Idempotency-Key -> один payment_id
CREATE TABLE IF NOT EXISTS idempotency_keys (
    key         TEXT PRIMARY KEY,
    payment_id  TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Состояние саги (orchestrator)
CREATE TABLE IF NOT EXISTS saga_state (
    payment_id   TEXT PRIMARY KEY,
    status       TEXT NOT NULL,   -- INITIATED | ANTIFRAUD_PENDING | LEDGER_PENDING | COMPLETED | FAILED
    step         TEXT,
    from_account TEXT,
    to_account   TEXT,
    amount       BIGINT,
    reason       TEXT,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Журнал уведомлений (notification)
CREATE TABLE IF NOT EXISTS notifications (
    id          BIGSERIAL PRIMARY KEY,
    payment_id  TEXT NOT NULL,
    recipient   TEXT,
    channel     TEXT,
    status      TEXT,
    text        TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
