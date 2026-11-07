-- Transactional outbox for settlement workflows. The Processor inserts a row
-- in the same transaction as a withdrawal's debit, so a debit can't commit
-- without its settlement, and a separate worker starts the workflows. Temporal
-- is never called from the event path.
CREATE TABLE settlements (
    event_id     TEXT        PRIMARY KEY,
    account_id   TEXT        NOT NULL,
    amount_minor BIGINT      NOT NULL,
    currency     CHAR(3)     NOT NULL,
    status       TEXT        NOT NULL DEFAULT 'pending'
                 CHECK (status IN ('pending', 'started', 'settled', 'reversed')),
    detail       TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The relay only ever looks for rows that haven't been started.
CREATE INDEX settlements_pending_idx ON settlements (created_at) WHERE status = 'pending';
