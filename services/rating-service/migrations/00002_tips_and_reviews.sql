-- +goose Up
ALTER TABLE driver_ratings ADD COLUMN IF NOT EXISTS tip_sum BIGINT NOT NULL DEFAULT 0;
ALTER TABLE driver_ratings ADD COLUMN IF NOT EXISTS tip_count INT NOT NULL DEFAULT 0;

-- One row per counted rating. event_id (the trip.rated eventId) is the dedupe
-- key; trip_id is unique too so a trip is never reviewed twice. Unlike
-- rating_events, these rows are the driver's review history: never pruned.
CREATE TABLE IF NOT EXISTS driver_reviews (
    event_id UUID PRIMARY KEY,
    trip_id TEXT NOT NULL UNIQUE,
    driver_id TEXT NOT NULL,
    rider_id TEXT NOT NULL,
    stars INT NOT NULL,
    comment TEXT NOT NULL DEFAULT '',
    tip_amount BIGINT NOT NULL DEFAULT 0 CHECK (tip_amount >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS driver_reviews_driver_created_idx ON driver_reviews (driver_id, created_at DESC);
