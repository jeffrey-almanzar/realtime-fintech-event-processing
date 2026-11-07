-- version increases by one on every change, so readers can discard stale
-- updates (e.g. one that arrives after a fresher snapshot).
ALTER TABLE accounts
    ADD COLUMN version       BIGINT      NOT NULL DEFAULT 1,
    ADD COLUMN last_event_id TEXT,
    ADD COLUMN last_event_at TIMESTAMPTZ;

CREATE FUNCTION accounts_bump_version() RETURNS trigger AS $$
BEGIN
    NEW.version := OLD.version + 1;
    RETURN NEW;
END
$$ LANGUAGE plpgsql;

CREATE TRIGGER accounts_bump_version
    BEFORE UPDATE ON accounts
    FOR EACH ROW EXECUTE FUNCTION accounts_bump_version();

-- Publishes every committed change on the account_updates channel. NOTIFY is
-- transactional: listeners only hear about changes that actually committed.
CREATE FUNCTION accounts_notify() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('account_updates', json_build_object(
        'account_id',    NEW.account_id,
        'currency',      NEW.currency,
        'balance_minor', NEW.balance_minor,
        'version',       NEW.version,
        'updated_at',    NEW.updated_at,
        'last_event_id', NEW.last_event_id,
        'last_event_at', NEW.last_event_at
    )::text);
    RETURN NULL;
END
$$ LANGUAGE plpgsql;

CREATE TRIGGER accounts_notify
    AFTER INSERT OR UPDATE ON accounts
    FOR EACH ROW EXECUTE FUNCTION accounts_notify();
