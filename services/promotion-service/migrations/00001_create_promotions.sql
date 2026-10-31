-- +goose Up

-- A promotion (voucher). Amounts are whole VND (VND has no minor unit). The code is
-- the primary key and is always upper case, so a rider typing "welcome10" finds it.
CREATE TABLE IF NOT EXISTS promotions (
    code TEXT PRIMARY KEY CHECK (code ~ '^[A-Z0-9_-]{3,32}$'),
    -- PERCENT: value is a whole percentage (1 to 100), capped by max_discount when set.
    -- FIXED: value is the VND taken off.
    kind TEXT NOT NULL CHECK (kind IN ('PERCENT', 'FIXED')),
    value BIGINT NOT NULL CHECK (value > 0),
    max_discount BIGINT CHECK (max_discount IS NULL OR max_discount > 0),
    -- The trip's price must be at least this much for the code to apply.
    min_fare BIGINT NOT NULL DEFAULT 0 CHECK (min_fare >= 0),
    -- NULL: any vehicle type.
    vehicle_type TEXT CHECK (vehicle_type IS NULL OR vehicle_type IN ('CAR', 'BIKE')),
    valid_from TIMESTAMPTZ NOT NULL,
    valid_to TIMESTAMPTZ NOT NULL,
    -- NULL: unlimited. Checked at validation and again at redemption; soft, see
    -- production-design.md ("Promotions").
    usage_limit BIGINT CHECK (usage_limit IS NULL OR usage_limit > 0),
    per_rider_limit BIGINT NOT NULL DEFAULT 1 CHECK (per_rider_limit > 0),
    -- Set when an admin disables the code. Redemptions already recorded stay.
    disabled_at TIMESTAMPTZ,
    created_by TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (valid_to > valid_from),
    CHECK (kind <> 'PERCENT' OR value <= 100),
    CHECK (kind = 'PERCENT' OR max_discount IS NULL)
);

-- One row per order that booked with a code, from order.created. The discount is
-- the one locked on the order (it is not recomputed here).
--   REDEEMED    counts against usage_limit and per_rider_limit.
--   OVER_LIMIT  a limit was already reached when this order was seen: the order was
--               booked with the discount anyway (it exists; nothing here can undo it),
--               so it is recorded but does not count. over_limit_reason says which limit.
--   RELEASED    the order was cancelled (order.cancelled): the redemption is given back.
CREATE TABLE IF NOT EXISTS redemptions (
    order_id TEXT NOT NULL,
    code TEXT NOT NULL,
    rider_id TEXT NOT NULL,
    discount BIGINT NOT NULL CHECK (discount >= 0),
    status TEXT NOT NULL CHECK (status IN ('REDEEMED', 'OVER_LIMIT', 'RELEASED')),
    over_limit_reason TEXT CHECK (over_limit_reason IS NULL OR over_limit_reason IN ('usage_limit', 'per_rider_limit')),
    source_event_id UUID NOT NULL,
    redeemed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    released_at TIMESTAMPTZ,
    PRIMARY KEY (code, order_id)
);
CREATE INDEX IF NOT EXISTS redemptions_rider_idx ON redemptions (code, rider_id) WHERE status = 'REDEEMED';
CREATE INDEX IF NOT EXISTS redemptions_order_idx ON redemptions (order_id);

-- Dedupe: an event (either topic) is applied at most once. Its row shares the
-- transaction with the effect, so "seen" never outlives an effect that failed to save.
CREATE TABLE IF NOT EXISTS promotion_events (
    event_id UUID PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Orders whose order.cancelled arrived before (or without) a redemption. order.created and
-- order.cancelled are different topics, so a quick cancel can be seen first; the redemption
-- is then recorded as RELEASED when order.created arrives. One row per cancelled order.
CREATE TABLE IF NOT EXISTS promotion_cancelled_orders (
    order_id TEXT PRIMARY KEY,
    cancelled_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- +goose Down
DROP TABLE IF EXISTS promotion_cancelled_orders;
DROP TABLE IF EXISTS promotion_events;
DROP TABLE IF EXISTS redemptions;
DROP TABLE IF EXISTS promotions;
