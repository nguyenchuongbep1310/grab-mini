-- +goose Up
-- Who cancelled a trip (RIDER or DRIVER) and an optional reason.
ALTER TABLE trips ADD COLUMN IF NOT EXISTS cancelled_by TEXT NOT NULL DEFAULT '';
ALTER TABLE trips ADD COLUMN IF NOT EXISTS cancel_reason TEXT NOT NULL DEFAULT '';

-- Orders the rider cancelled (order.cancelled). order.matched and
-- order.cancelled travel on different topics, so a cancellation can be seen
-- before the trip it should cancel exists; remembering the order lets the late
-- order.matched see it and not create that trip.
CREATE TABLE IF NOT EXISTS cancelled_orders (
    order_id TEXT PRIMARY KEY,
    cancelled_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
