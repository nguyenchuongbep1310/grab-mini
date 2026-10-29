-- +goose Up

-- ---------------------------------------------------------------- ledger ----
-- Double-entry: every ledger_transactions row has entries that sum to exactly
-- zero. Amounts are signed integer minor units (VND has none: whole dong);
-- negative takes value out of the account, positive puts it in.
CREATE TABLE IF NOT EXISTS ledger_accounts (
    id TEXT PRIMARY KEY,
    kind TEXT NOT NULL CHECK (kind IN ('RIDER', 'DRIVER_PAYABLE', 'PLATFORM_REVENUE')),
    owner_id TEXT NOT NULL DEFAULT '',
    currency TEXT NOT NULL DEFAULT 'VND',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ledger_transactions (
    id UUID PRIMARY KEY,
    -- One logical charge ("fare:<tripId>") is booked at most once.
    idempotency_key TEXT NOT NULL UNIQUE,
    kind TEXT NOT NULL,
    currency TEXT NOT NULL DEFAULT 'VND',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ledger_entries (
    id BIGSERIAL PRIMARY KEY,
    txn_id UUID NOT NULL REFERENCES ledger_transactions (id),
    account_id TEXT NOT NULL REFERENCES ledger_accounts (id),
    amount BIGINT NOT NULL CHECK (amount <> 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ledger_entries_txn_idx ON ledger_entries (txn_id);
CREATE INDEX IF NOT EXISTS ledger_entries_account_idx ON ledger_entries (account_id);

-- Second line of defense behind the check in the service code: when a
-- transaction commits, every ledger transaction it touched must sum to zero.
-- Deferred, so the entries of one transaction can be inserted one at a time.
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION ledger_assert_balanced() RETURNS trigger AS $$
DECLARE
    total BIGINT;
BEGIN
    SELECT COALESCE(SUM(amount), 0) INTO total FROM ledger_entries WHERE txn_id = NEW.txn_id;
    IF total <> 0 THEN
        RAISE EXCEPTION 'ledger transaction % is not balanced (sums to %)', NEW.txn_id, total
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

DROP TRIGGER IF EXISTS ledger_entries_balanced ON ledger_entries;
CREATE CONSTRAINT TRIGGER ledger_entries_balanced
    AFTER INSERT ON ledger_entries
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION ledger_assert_balanced();

-- The ledger is append-only: a correction is a new, reversing transaction.
-- (TRUNCATE is not row-level, so tests can still reset the tables.)
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION ledger_append_only() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'the ledger is append-only: % on % is not allowed', TG_OP, TG_TABLE_NAME;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

DROP TRIGGER IF EXISTS ledger_entries_immutable ON ledger_entries;
CREATE TRIGGER ledger_entries_immutable
    BEFORE UPDATE OR DELETE ON ledger_entries
    FOR EACH ROW EXECUTE FUNCTION ledger_append_only();

DROP TRIGGER IF EXISTS ledger_transactions_immutable ON ledger_transactions;
CREATE TRIGGER ledger_transactions_immutable
    BEFORE UPDATE OR DELETE ON ledger_transactions
    FOR EACH ROW EXECUTE FUNCTION ledger_append_only();

-- -------------------------------------------------------------- payments ----
-- One row per charge: a trip has at most one FARE, one TIP and one
-- CANCELLATION_FEE, whatever events are redelivered or replayed.
CREATE TABLE IF NOT EXISTS payments (
    id UUID PRIMARY KEY,
    trip_id TEXT NOT NULL,
    order_id TEXT NOT NULL,
    rider_id TEXT NOT NULL,
    driver_id TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('FARE', 'TIP', 'CANCELLATION_FEE')),
    status TEXT NOT NULL CHECK (status IN ('CAPTURED', 'DECLINED')),
    amount BIGINT NOT NULL CHECK (amount > 0),
    driver_amount BIGINT NOT NULL DEFAULT 0 CHECK (driver_amount >= 0),
    platform_amount BIGINT NOT NULL DEFAULT 0 CHECK (platform_amount >= 0),
    currency TEXT NOT NULL DEFAULT 'VND',
    provider TEXT NOT NULL,
    provider_ref TEXT NOT NULL DEFAULT '',
    decline_reason TEXT NOT NULL DEFAULT '',
    -- Set when CAPTURED. Deferred so the payment can be inserted before the
    -- ledger transaction within one database transaction.
    ledger_txn_id UUID REFERENCES ledger_transactions (id) DEFERRABLE INITIALLY DEFERRED,
    source_event_id UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (trip_id, kind),
    CHECK (status <> 'CAPTURED' OR (driver_amount + platform_amount = amount AND ledger_txn_id IS NOT NULL)),
    CHECK (status <> 'DECLINED' OR (driver_amount = 0 AND platform_amount = 0 AND ledger_txn_id IS NULL))
);
CREATE INDEX IF NOT EXISTS payments_rider_idx ON payments (rider_id, created_at DESC);
CREATE INDEX IF NOT EXISTS payments_driver_idx ON payments (driver_id, created_at DESC);

-- Dedupe: an event (any of the three topics) is applied at most once. Its row
-- shares the transaction with the payment and ledger writes.
CREATE TABLE IF NOT EXISTS payment_events (
    event_id UUID PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- A local copy of what an order costs, from order.created (the price is locked
-- at booking). price is NULL for an order created before prices existed.
CREATE TABLE IF NOT EXISTS order_prices (
    order_id TEXT PRIMARY KEY,
    rider_id TEXT NOT NULL,
    price BIGINT CHECK (price IS NULL OR price >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
