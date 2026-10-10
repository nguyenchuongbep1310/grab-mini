-- +goose Up
-- Surge pricing: the multiplier applied and the price locked at booking (VND).
-- Orders booked before surge existed keep multiplier 1.0 and an unknown (NULL) price.
ALTER TABLE orders ADD COLUMN IF NOT EXISTS surge_multiplier NUMERIC NOT NULL DEFAULT 1.0;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS price NUMERIC NULL;
