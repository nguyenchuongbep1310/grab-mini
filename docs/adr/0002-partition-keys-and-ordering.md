# 0002. Partition by the entity whose order matters

Status: accepted

## Context

Kafka orders messages only within a partition. Consumers in a group scale by partition. We need both: per-entity ordering where correctness depends on it, and enough partitions to spread load.

## Decision

| Topic | Key | Why this key |
|---|---|---|
| `order.created`, `order.matched`, `order.no_driver_found`, `order.cancelled` | `orderId` | An order's events must not be reordered (a cancel must not overtake its creation) |
| `trip.status.changed` | `tripId` | Status transitions of one trip are ordered |
| `trip.rated` | `driverId` | Rating Service aggregates per driver |
| `driver.location.updated` | `driverId` | A driver's positions arrive in the order sent, so the newest wins |

There is no global ordering and none is needed: nothing compares events of two different orders.

Partition counts in the Kubernetes manifests (`deploy/k8s/base/kafka/topics.yaml`): 12 for the order and trip topics, 6 for low-volume ones, 48 for `driver.location.updated`. Local Docker Compose uses 3.

## Alternatives considered

- **One partition per topic.** Total order, but one consumer per group; no scaling.
- **Random/round-robin keys.** Best balance, but loses per-order ordering and makes duplicates harder to reason about.

## Consequences

- Good: ordering where it matters, parallelism across entities. Measured: matching with one worker per partition (12 partitions, 12 workers) was correctly ordered within a partition and scaled past the single-loop limit ([performance.md](../performance.md)).
- Bad: partition count is a ceiling on consumer parallelism and is hard to change later (changing it re-maps keys, breaking ordering during the change). 12/48 were chosen for headroom, not measured on real hardware.
- Bad: a hot key (one driver, one order) cannot be spread. Not a problem at this cardinality; it would be for something like "per city".
- Retry topics ([0006](0006-retry-tiers-and-dead-letter-topics.md)) break ordering for a retried message by design; handlers guard state changes by current status instead.

## Revisit when

A consumer group needs more parallelism than its partition count, or a topic needs ordering across entities.
