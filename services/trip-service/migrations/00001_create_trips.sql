-- +goose Up
CREATE TABLE IF NOT EXISTS trips (
    id UUID PRIMARY KEY,
    order_id TEXT NOT NULL UNIQUE,
    driver_id TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    cancelled_at TIMESTAMPTZ
);

-- US-6.1: dedupe table so a redelivered Kafka event never creates a
-- second trip for the same order.matched event.
CREATE TABLE IF NOT EXISTS processed_events (
    event_id UUID PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
