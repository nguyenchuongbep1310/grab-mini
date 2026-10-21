# 0004. Transactional outbox for publishing events

Status: accepted

## Context

"Write the row, then publish to Kafka" has a failure window between the two steps: an order exists that nobody will ever match, or a completed trip the rider never hears about. A distributed transaction across Postgres and Kafka is not available.

## Decision

Order and Trip services insert the event into an `outbox` table in the same Postgres transaction as the state change. A relay (`services/pkg/outbox`) polls (every 1 s, 100 rows per batch), publishes to Kafka, and deletes the rows it sent. One relay publishes at a time per source, guarded by `pg_try_advisory_xact_lock`, in insertion order, so events for one key reach Kafka in commit order.

## Alternatives considered

- **Publish after commit, best effort.** Simple, loses events on a crash at the wrong moment.
- **Change data capture (Debezium).** No polling and no relay code, but another cluster component to run. Reasonable at scale; heavy for this project.
- **Publish first, then write.** Produces events for state that may never exist.

## Consequences

- Good: no phantom events (a rolled-back transaction publishes nothing) and no lost ones (a failed publish leaves the rows queued); both covered by tests against real Postgres, plus a real-broker test. An event Kafka will never accept is quarantined so it does not block the rest.
- Good: the relay is just Go in the service, so the failure mode is visible in metrics (`outbox_published_total`, backlog age alert).
- Bad: at-least-once, so consumers must dedupe ([0003](0003-at-least-once-and-idempotent-consumers.md)).
- Bad: added latency (up to the 1 s poll interval) and Postgres write load for the outbox table.
- Bad: a single relay per source is a throughput ceiling and a single point of delay: it is the first thing I would measure on the order path ([scaling.md](../scaling.md)).

## Revisit when

Outbox backlog age grows under load, or polling cost shows up in Postgres. Then batch/partition the relay by key hash, or move to CDC.
