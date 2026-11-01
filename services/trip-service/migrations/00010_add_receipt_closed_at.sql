-- +goose Up
-- When the rider closed the receipt of a finished ride (or rated it, which
-- closes it too). NULL while it is still open; written once, never moved.
ALTER TABLE trips ADD COLUMN IF NOT EXISTS receipt_closed_at TIMESTAMPTZ;
