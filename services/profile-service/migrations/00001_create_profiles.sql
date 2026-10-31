-- +goose Up

-- Rider profiles. PERSONAL DATA: a name and a phone number, typed by the rider. Never logged.
-- The phone is not verified (no SMS code): it is whatever the rider entered.
CREATE TABLE IF NOT EXISTS rider_profiles (
    rider_id TEXT PRIMARY KEY,
    name TEXT NOT NULL CHECK (char_length(name) BETWEEN 1 AND 80),
    phone TEXT NOT NULL DEFAULT '' CHECK (char_length(phone) <= 20),
    updated_at TIMESTAMPTZ NOT NULL
);

-- Driver profiles and their onboarding state. PERSONAL DATA: name, phone, plate. Never logged.
--
-- status:
--   NONE      the driver saved a profile but never applied
--   PENDING   applied (or changed the vehicle after approval); waiting for an admin
--   APPROVED  may be offered trips
--   REJECTED  an admin said no (reason says why); may apply again
--   SUSPENDED an admin took the driver out of service; only an admin brings them back
-- Past NONE, name, vehicle type and plate are always present: a profile that is under review
-- or approved cannot be edited into an incomplete one (the CHECK is the last line of defence).
--
-- reviewed_at is the time of the last review decision for the driver, which is the
-- occurredAt of the event published for it (the development seed sets it without an event). Every new result is later than the previous one, which is what lets Matching
-- Service ignore a result that arrives out of order. status_at is when the status last
-- changed: the admin queue is oldest first by it.
CREATE TABLE IF NOT EXISTS driver_profiles (
    driver_id TEXT PRIMARY KEY,
    name TEXT NOT NULL DEFAULT '' CHECK (char_length(name) <= 80),
    phone TEXT NOT NULL DEFAULT '' CHECK (char_length(phone) <= 20),
    vehicle_type TEXT NOT NULL DEFAULT '' CHECK (vehicle_type IN ('', 'CAR', 'BIKE')),
    plate TEXT NOT NULL DEFAULT '' CHECK (char_length(plate) <= 16),
    model TEXT NOT NULL DEFAULT '' CHECK (char_length(model) <= 50),
    color TEXT NOT NULL DEFAULT '' CHECK (char_length(color) <= 30),
    status TEXT NOT NULL DEFAULT 'NONE' CHECK (status IN ('NONE', 'PENDING', 'APPROVED', 'REJECTED', 'SUSPENDED')),
    reason TEXT NOT NULL DEFAULT '' CHECK (char_length(reason) <= 500),
    reviewed_by TEXT NOT NULL DEFAULT '',
    reviewed_at TIMESTAMPTZ,
    status_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CHECK (status = 'NONE' OR (name <> '' AND vehicle_type <> '' AND plate <> ''))
);
-- The admin queue: one status, oldest first, paged by (status_at, driver_id).
CREATE INDEX IF NOT EXISTS driver_profiles_queue_idx ON driver_profiles (status, status_at, driver_id);

-- +goose Down
DROP TABLE IF EXISTS driver_profiles;
DROP TABLE IF EXISTS rider_profiles;
