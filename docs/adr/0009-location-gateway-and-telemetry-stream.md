# 0009. Treat location telemetry differently from transactional events

Status: accepted

## Context

Driver locations are by far the highest-volume stream (design target 25,000 per second) and the least valuable per message: the next one replaces the last. Orders and trips are low volume and must never be lost. Treating them the same wastes money on one and risks the other.

## Decision

- **Drivers never write to Kafka.** A client cannot be trusted with Kafka credentials, clock, rate or coordinates. `location-gateway` authenticates the driver (JWT), validates coordinates, assigns the server timestamp, rate-limits per driver, and drops redundant reports: a driver who has not moved more than a minimum distance is accepted but not published, except for a heartbeat so a position never goes stale.
- **Lower durability for telemetry.** `driver.location.updated` is written with `RequiredAcks=One` (the order/trip/matching writers use `All`), keeps 1 day of data (others 7 days), `min.insync.replicas` 1 (others 2) and has 48 partitions in the Kubernetes manifests.
- **Latest wins.** Driver Location Service keeps only the newest timestamp per driver in Redis GEO and drops late events ([0003](0003-at-least-once-and-idempotent-consumers.md)).

## Alternatives considered

- **Same durability as transactional events.** Safer but pays for guarantees nobody needs on a stream that is superseded in seconds.
- **Drivers produce directly (Kafka REST proxy).** Hands clients control over rate, clock and key.
- **Gateway writes straight to Redis.** Skips Kafka, but then ksqlDB, notification and any future consumer lose the stream.

## Consequences

- Good: measured consumer capacity ~5,000 messages/s on one box and 25,000/s needing only about a quarter of a Redis core for `GEOADD` ([performance.md](../performance.md)); the gateway cuts traffic from stationary drivers.
- Bad: a leader failure can lose a few recent location messages (acks=1). Accepted: the next report corrects it.
- Bad: the gateway keeps per-driver state in memory, so the rate limiter and the stationary filter are per instance. A driver reconnecting to another instance resets their window; with a sticky or hashed load balancer this is fine, otherwise limits are looser than configured ([scaling.md](../scaling.md)).

## Revisit when

A feature needs every location point (route history or billing by distance): that stream would then need transactional durability and its own topic.
