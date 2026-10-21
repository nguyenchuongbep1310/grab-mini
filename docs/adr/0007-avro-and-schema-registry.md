# 0007. Avro events in Schema Registry with BACKWARD compatibility

Status: accepted

## Context

Services are deployed independently, so a producer will sometimes be newer than its consumers or the other way round. Plain JSON hides breaking changes until a consumer fails in production.

## Decision

Every event is Avro in Confluent's wire format (magic byte, 4-byte schema id, payload). Schemas live in `services/pkg/events/schemas/*.avsc`, one per topic, under the subject `<topic>-value`. The registry runs in `BACKWARD` mode: a new schema must be able to read data written with the old one, so in practice a new field needs a default. A consumer reads the writer's schema id from each message and resolves it against its own schema.

Worked examples: `vehicleType` added to `order.created`/`order.matched` as version 2 with default `CAR`; `tipAmount`/`currency` added to `trip.rated`; `surgeMultiplier`/`price` added to `order.created` (v3). A test shows an old-schema message still decoding with the default.

Docker Compose registers all schemas before anything is produced (ksqlDB infers columns from the registry).

## Alternatives considered

- **JSON with a convention.** Easiest; no enforcement. Breaks silently.
- **Protobuf.** Equally valid, and better tooling in some stacks. Avro was picked because it is the natural fit for Schema Registry and ksqlDB here; this is a preference, not a measured one.

## Consequences

- Good: an incompatible change is rejected by the registry (HTTP 409) at registration, not discovered by a crashing consumer; verified by a test and a guided exercise in the [developer guide](../guide.md).
- Bad: all producers share one library and must register at startup, so the registry is a startup dependency. Schemas are also registered up front by `schema-init`.
- Bad: **compatibility is only enforced at registration time, and `BACKWARD` rather than `BACKWARD_TRANSITIVE`.** Nothing in CI yet checks a changed `.avsc` against the registry before merge, and a long-lived consumer far behind could still meet a message it cannot resolve. Both are known gaps.
- Bad: events are not human-readable in the log without a deserializer (Kafka UI and ksqlDB have one).

## Revisit when

There are multiple teams producing to the same topic: add a CI compatibility check and move to `BACKWARD_TRANSITIVE` or `FULL`.
