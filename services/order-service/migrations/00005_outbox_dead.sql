-- +goose Up
-- Events Kafka will never accept (too large for the broker, say), moved aside by
-- the outbox relay so they don't block the ones behind them. A person looks at
-- these: see docs/runbook.md#outbox-backlog.
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
