CREATE TABLE accounts (
    account_id    TEXT        PRIMARY KEY,
    currency      CHAR(3)     NOT NULL,
    -- Minor units (e.g. cents). Never store money as floating point.
    balance_minor BIGINT      NOT NULL DEFAULT 0,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- One row per applied event. The primary key on event_id is what makes
-- processing idempotent: a redelivered event conflicts and is skipped.
CREATE TABLE processed_events (
    event_id        TEXT        PRIMARY KEY,
    account_id      TEXT        NOT NULL,
    type            TEXT        NOT NULL,
    amount_minor    BIGINT      NOT NULL,
    currency        CHAR(3)     NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL,
    processed_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    kafka_partition INT         NOT NULL,
    kafka_offset    BIGINT      NOT NULL
);

CREATE INDEX processed_events_account_created_idx
    ON processed_events (account_id, created_at DESC);
