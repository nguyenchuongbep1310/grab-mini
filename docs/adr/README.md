# Architecture decision records

Each record states the problem, what was chosen, what was given up, and what would make me revisit it. They describe what the code in this repository does today; where something is only a design intention or has not been verified at scale, the record says so.

| # | Decision | One line |
|---|---|---|
| [0001](0001-kafka-event-driven-architecture.md) | Kafka as the backbone | Services talk through events, not calls, so a slow or dead consumer never blocks the order path |
| [0002](0002-partition-keys-and-ordering.md) | Partition keys | Key by the entity whose order matters (`orderId`, `tripId`, `driverId`); no global ordering |
| [0003](0003-at-least-once-and-idempotent-consumers.md) | Delivery semantics | At-least-once plus idempotent consumers instead of Kafka exactly-once transactions |
| [0004](0004-transactional-outbox.md) | Transactional outbox | State and event are committed in one Postgres transaction; a relay publishes afterwards |
| [0005](0005-redis-atomic-driver-claim.md) | Driver matching | One Redis Lua script claims a driver, with a lease, so two orders never get the same driver |
| [0006](0006-retry-tiers-and-dead-letter-topics.md) | Failure handling | Retry topics with growing delay, then a DLQ; permanent errors skip retries; operator redrive |
| [0007](0007-avro-and-schema-registry.md) | Event contracts | Avro in Schema Registry, `BACKWARD` compatibility, defaults for new fields |
| [0008](0008-notification-fan-out-and-recovery.md) | Realtime delivery | WebSocket pushes fanned out through Redis pub/sub; push is best-effort, REST is the recovery path |
| [0009](0009-location-gateway-and-telemetry-stream.md) | Location telemetry | Drivers never write to Kafka; a gateway rate-limits and drops redundant reports |
| [0010](0010-payment-ledger-and-provider-idempotency.md) | Payments | Double-entry ledger in Postgres; the card provider sits behind an idempotent interface; no float money |

See also [scaling.md](../scaling.md) (what breaks first at 10x) and [performance.md](../performance.md) (what was measured).

Template: *Context*, *Decision*, *Alternatives considered*, *Consequences* (good and bad), *Revisit when*.
