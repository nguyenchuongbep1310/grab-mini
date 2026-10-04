-- +goose Up
-- Ratings arrived after the first release.
ALTER TABLE trips ADD COLUMN IF NOT EXISTS rating INT;
ALTER TABLE trips ADD COLUMN IF NOT EXISTS rating_comment TEXT;
ALTER TABLE trips ADD COLUMN IF NOT EXISTS rated_at TIMESTAMPTZ;
