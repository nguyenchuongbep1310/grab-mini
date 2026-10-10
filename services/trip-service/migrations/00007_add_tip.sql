-- +goose Up
-- Tip is a recorded amount in whole VND; no payment system moves it.
ALTER TABLE trips ADD COLUMN IF NOT EXISTS tip_amount BIGINT NOT NULL DEFAULT 0 CHECK (tip_amount >= 0);
