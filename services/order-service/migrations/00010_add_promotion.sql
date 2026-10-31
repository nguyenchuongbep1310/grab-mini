-- +goose Up
-- Promotions: the code the rider booked with and the discount (whole VND) locked on the
-- order together with the price. Payment Service charges price - discount. Orders booked
-- before promotions existed keep no code and a discount of 0.
ALTER TABLE orders ADD COLUMN IF NOT EXISTS promo_code TEXT NOT NULL DEFAULT '';
ALTER TABLE orders ADD COLUMN IF NOT EXISTS discount BIGINT NOT NULL DEFAULT 0 CHECK (discount >= 0);
