-- +goose Up
CREATE TABLE IF NOT EXISTS driver_ratings (
    driver_id TEXT PRIMARY KEY,
    rating_count INT NOT NULL,
    rating_sum INT NOT NULL
);

-- Dedupe: a redelivered trip.rated must not be counted twice.
CREATE TABLE IF NOT EXISTS rating_events (
    event_id UUID PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
