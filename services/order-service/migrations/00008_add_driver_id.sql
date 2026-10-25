-- +goose Up
-- The driver an order was matched to (from order.matched), so only that driver
-- may read the order's addresses. Empty until matched, and on orders matched
-- before this column existed: no driver can read those.
ALTER TABLE orders ADD COLUMN IF NOT EXISTS driver_id TEXT NOT NULL DEFAULT '';
