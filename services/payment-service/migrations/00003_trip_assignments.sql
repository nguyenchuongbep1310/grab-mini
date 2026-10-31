-- +goose Up

-- When each trip got its driver, from the trip's ASSIGNED event. A rider who cancels
-- shortly after that pays no cancellation fee (CANCELLATION_GRACE_SECONDS), so the
-- cancel needs to know how long the driver had been committed. One row per trip.
CREATE TABLE IF NOT EXISTS trip_assignments (
    trip_id TEXT PRIMARY KEY,
    assigned_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- +goose Down
DROP TABLE IF EXISTS trip_assignments;
