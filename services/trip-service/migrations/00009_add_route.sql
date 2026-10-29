-- +goose Up
-- Where the trip starts and ends, copied from order.matched, so pickup and
-- complete can check the driver is actually there. NULL on trips created
-- before this column existed (they skip the proximity check).
ALTER TABLE trips ADD COLUMN IF NOT EXISTS pickup_lat DOUBLE PRECISION;
ALTER TABLE trips ADD COLUMN IF NOT EXISTS pickup_lng DOUBLE PRECISION;
ALTER TABLE trips ADD COLUMN IF NOT EXISTS dropoff_lat DOUBLE PRECISION;
ALTER TABLE trips ADD COLUMN IF NOT EXISTS dropoff_lng DOUBLE PRECISION;
