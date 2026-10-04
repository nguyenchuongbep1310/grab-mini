-- +goose Up
-- Who to tell about the trip; copied from order.matched (empty for older trips).
ALTER TABLE trips ADD COLUMN IF NOT EXISTS rider_id TEXT NOT NULL DEFAULT '';
