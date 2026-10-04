-- +goose Up
-- The transactional outbox (see pkg/outbox): events saved in the same
-- transaction as the order, published to Kafka afterwards.
CREATE TABLE IF NOT EXISTS outbox (
    id BIGSERIAL PRIMARY KEY,
    source TEXT NOT NULL,
    topic TEXT NOT NULL,
    key BYTEA,
    payload BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS outbox_source_id ON outbox (source, id);
