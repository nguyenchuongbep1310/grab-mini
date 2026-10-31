-- +goose Up

-- The discount (whole VND) a promo code took off the order, from order.created (v4,
-- `discount`). The rider is charged price - discount, and the platform commission is taken on
-- that net amount. 0 for an order booked without a code and for one created before promotions.
ALTER TABLE order_prices ADD COLUMN IF NOT EXISTS discount BIGINT NOT NULL DEFAULT 0 CHECK (discount >= 0);

-- +goose Down
ALTER TABLE order_prices DROP COLUMN IF EXISTS discount;
