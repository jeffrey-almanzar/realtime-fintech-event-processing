DROP TRIGGER IF EXISTS accounts_notify ON accounts;
DROP FUNCTION IF EXISTS accounts_notify();
DROP TRIGGER IF EXISTS accounts_bump_version ON accounts;
DROP FUNCTION IF EXISTS accounts_bump_version();

ALTER TABLE accounts
    DROP COLUMN IF EXISTS last_event_at,
    DROP COLUMN IF EXISTS last_event_id,
    DROP COLUMN IF EXISTS version;
