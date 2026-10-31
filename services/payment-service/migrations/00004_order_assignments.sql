-- +goose Up

-- trip_assignments (00003) waited for a trip.status.changed ASSIGNED event, but Trip Service
-- never publishes one: it creates the trip silently when it consumes order.matched. The moment
-- the driver is assigned is order.matched (Matching Service stamps it), which carries the
-- orderId the later cancellation event also carries. Nothing was ever written to the old table.
DROP TABLE IF EXISTS trip_assignments;

-- When each order got its driver, from order.matched. A rider who cancels shortly after that
-- pays no cancellation fee (CANCELLATION_GRACE_SECONDS). One row per order.
CREATE TABLE IF NOT EXISTS order_assignments (
    order_id TEXT PRIMARY KEY,
    assigned_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- +goose Down
DROP TABLE IF EXISTS order_assignments;
