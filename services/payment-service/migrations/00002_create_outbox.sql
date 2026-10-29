-- +goose Up
-- The transactional outbox (see pkg/outbox): payment.recorded is saved in the
-- same transaction as the payment and its ledger entries, published to Kafka
-- afterwards. Must stay in step with the table in pkg/outbox (as in
-- trip-service's migrations 00004 to 00006); IF NOT EXISTS because services
-- share a database in development.
CREATE TABLE IF NOT EXISTS outbox (
    id BIGSERIAL PRIMARY KEY,
    source TEXT NOT NULL,
    topic TEXT NOT NULL,
    key BYTEA,
    payload BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE outbox ADD COLUMN IF NOT EXISTS headers JSONB;
CREATE INDEX IF NOT EXISTS outbox_source_id ON outbox (source, id);

CREATE TABLE IF NOT EXISTS outbox_dead (
    id BIGSERIAL PRIMARY KEY,
    source TEXT NOT NULL,
    topic TEXT NOT NULL,
    key BYTEA,
    payload BYTEA NOT NULL,
    headers JSONB,
    created_at TIMESTAMPTZ NOT NULL,
    failed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    error TEXT NOT NULL
);
