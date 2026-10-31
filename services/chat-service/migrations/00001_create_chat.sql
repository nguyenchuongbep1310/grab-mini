-- +goose Up

-- Who may talk about a trip, learned from trip.status.changed. One row per trip.
-- rider_id is '' for a trip whose events predate the field; such a trip has no
-- rider who can talk until a later event names one.
-- status only moves forward: ASSIGNED, IN_PROGRESS, then COMPLETED or CANCELLED (the
-- end, which never moves again). updated_at is when that status was reached (the
-- event's occurredAt): it is the start of the read-only grace period after an end.
CREATE TABLE IF NOT EXISTS chat_trips (
    trip_id TEXT PRIMARY KEY,
    rider_id TEXT NOT NULL DEFAULT '',
    driver_id TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('ASSIGNED', 'IN_PROGRESS', 'COMPLETED', 'CANCELLED')),
    updated_at TIMESTAMPTZ NOT NULL
);
-- The retention job finds finished trips by age.
CREATE INDEX IF NOT EXISTS chat_trips_finished_idx ON chat_trips (updated_at) WHERE status IN ('COMPLETED', 'CANCELLED');

-- The messages. PERSONAL DATA: text is what people typed. It is never logged, and
-- the retention job (CHAT_RETENTION_DAYS) deletes it.
-- client_key is the optional Idempotency-Key of the request that sent it: a retry
-- with the same key returns this row instead of adding another.
CREATE TABLE IF NOT EXISTS chat_messages (
    id UUID PRIMARY KEY,
    trip_id TEXT NOT NULL,
    sender_id TEXT NOT NULL,
    sender_role TEXT NOT NULL CHECK (sender_role IN ('rider', 'driver')),
    text TEXT NOT NULL CHECK (char_length(text) BETWEEN 1 AND 500),
    client_key TEXT,
    created_at TIMESTAMPTZ NOT NULL
);
-- History is read newest first, paged by (created_at, id).
CREATE INDEX IF NOT EXISTS chat_messages_trip_idx ON chat_messages (trip_id, created_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS chat_messages_age_idx ON chat_messages (created_at);
CREATE UNIQUE INDEX IF NOT EXISTS chat_messages_client_key_idx ON chat_messages (trip_id, sender_id, client_key) WHERE client_key IS NOT NULL;

-- Dedupe: a trip.status.changed event is applied at most once. Its row shares the
-- transaction with the effect. Pruned by the retention CronJob (deploy/k8s/generate.py).
CREATE TABLE IF NOT EXISTS chat_events (
    event_id UUID PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- +goose Down
DROP TABLE IF EXISTS chat_events;
DROP TABLE IF EXISTS chat_messages;
DROP TABLE IF EXISTS chat_trips;
