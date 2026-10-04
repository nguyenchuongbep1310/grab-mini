-- +goose Up
-- The trace context of whatever queued the event (see pkg/tracing), so a trace
-- continues through the wait for the relay and on into Kafka.
ALTER TABLE outbox ADD COLUMN IF NOT EXISTS headers JSONB;
