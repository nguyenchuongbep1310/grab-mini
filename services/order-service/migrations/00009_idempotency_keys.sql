-- +goose Up
-- Idempotency-Key for POST /orders: a client that retries a booking (network
-- error, 5xx) with the same key gets the original response back instead of a
-- second order. The row is written in the same transaction as the order and
-- its outbox event, and the primary key is what stops two concurrent requests
-- with the same key from both creating an order.
--
-- A key is honoured for 24 hours (idempotencyTTL in idempotency.go): an older
-- row is ignored and replaced. order-service also deletes rows older than that
-- in the background, so the table stays small.
CREATE TABLE IF NOT EXISTS idempotency_keys (
    rider_id TEXT NOT NULL,
    key TEXT NOT NULL,
    order_id UUID NOT NULL,
    -- SHA-256 of the normalized request body: the same key with another body is a 422.
    fingerprint TEXT NOT NULL,
    status_code INT NOT NULL,
    -- The response body as first sent, replayed verbatim.
    response BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (rider_id, key)
);
CREATE INDEX IF NOT EXISTS idempotency_keys_created_at ON idempotency_keys (created_at);
