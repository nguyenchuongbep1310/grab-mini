# Production design: Ride-booking Mini on Kafka

This document describes how to take grab-mini from the learning version (1 broker, 3 partitions, running locally) to a system that can run in production. It **does not change code**: every "current" statement below was checked against the code in `services/`. The load figures are **assumptions** used for calculation, not measurements.

References: [design.md](./design.md) (design of the learning version), [requirements.md](./requirements.md).

## 1. Goals and assumptions

| | Assumption |
|---|---|
| Drivers online at the same time | 100,000 |
| Location report frequency | 1 per 4 seconds, i.e. about 25,000 msg/s |
| Peak booking rate | 500 orders/s |
| Event size | ~150 bytes (Avro) for locations, ~300 bytes for other events |
| Availability | 99.9% for the booking path (create, match, trip) |
| Match latency | p95 under 3 seconds from `order.created` to `order.matched` |
| Data loss | Orders, trips and ratings must not be lost. Some location loss is acceptable (only the latest value matters) |

From these assumptions: the location stream is ~2.5 MB/s, small compared with what Kafka can handle. The main production problem is **not throughput** but **correctness under failure** (section 3) and **operations** (sections 6 to 8).

## 2. Two kinds of streams, two different policies

Putting every topic under a single configuration is a common mistake. Split them into two groups:

| | Transactional stream (business events) | Telemetry stream (locations) |
|---|---|---|
| Topics | `order.*`, `trip.*` | `driver.location.updated` |
| Message loss | Not acceptable | Acceptable |
| Producer `acks` | `all` | `1` |
| `min.insync.replicas` | 2 | 1 |
| Idempotent producer | On | Not needed |
| Retention | 7 days (enough to replay after a weekend incident) | 1 day or less |
| Compression | `lz4` | `lz4` or `zstd` |
| Compaction | No | `cleanup.policy=compact` can be used (key = driverId) if the "latest location" is needed when a consumer restarts |

Currently the `kafka.Writer` in every service does not set `RequiredAcks`, so it uses the kafka-go default (`RequireNone`). In production this must be set explicitly: `RequireAll` for the transactional stream.

## 3. Correctness: gaps in the current version and how to close them

This is the most important part. Each item has: the symptom, the location in the code, the failure scenario, and the fix.

### 3.1 DB write followed by publish is not atomic (dual write)

**Currently.** `order-service/main.go` `handleCreateOrder`: `INSERT` the order, and only then `WriteMessages`. If the publish fails or the process dies between the two steps, the order stays `PENDING` forever and nobody matches it.

It is worse in Trip Service: `handleTransition` and `handleRateTrip` only `log.Printf` when the publish fails and still return success. The trip has moved to `COMPLETED` in the DB but `trip.status.changed` is never sent, so the rider gets no notification and ksqlDB undercounts. For `trip.rated`, the rating is saved in `trips` but Rating Service never receives it, so the average score is wrong permanently.

**Solution: Transactional Outbox.**

```
BEGIN
  INSERT INTO orders ...
  INSERT INTO outbox (id, topic, key, payload, created_at) ...
COMMIT
        │
        ▼ (relay reads outbox, publishes, marks as sent)
     Kafka
```

- The `outbox` table lives in the same database and the same transaction as the business data, so either both are written or neither is.
- The relay has two options: (a) a goroutine that polls the outbox table in batches (`SELECT ... FOR UPDATE SKIP LOCKED`), which is simple; (b) CDC with Debezium reading the Postgres WAL, which has low latency and puts no load on the DB, but adds one more component to operate. Recommendation: start with (a).
- The relay publishes **at-least-once**, so consumers must be idempotent. The current version already has `eventId` and dedupe, so this condition is already met.
- The Avro payload is encoded before it is stored, which preserves the existing "encode first, write after" principle.

### 3.2 Matching: claim the driver and then publish, and not idempotent on redelivery

**Currently.** `matching-service/main.go`: claims the driver with a Lua script and then publishes `order.matched`. There are two gaps:

1. If the process dies **after the claim, before the publish**, the driver is stuck `ASSIGNED` forever (the code only recovers when the publish returns an error, not when the process dies).
2. If the process dies **after the publish, before the offset commit**, `order.created` is redelivered and matching runs a second time: it claims **another driver** for the same order. Trip Service can block the second trip thanks to `order_id UNIQUE`, but the second driver has already been claimed and nobody releases it.

**Solution.**
- Store the match decision keyed by `orderId` as an idempotency key: before claiming, `SET match:{orderId} <driverId> NX EX 3600`. If the key already exists, republish `order.matched` with the stored driver instead of making a new claim.
- Put a TTL on the `ASSIGNED` state (for example 60 seconds). If it expires with no trip, the driver returns to `AVAILABLE` on its own. This also covers the case where the driver loses connectivity. Trip Service extends it or switches to `ON_TRIP` when it creates the trip.
- Use the outbox, or write `match:{orderId}` and the event in the same logical step, to keep the window as small as possible.

### 3.3 The retry loop blocks the partition

**Currently.** `pkg/kafkaconsumer`: retries with `time.Sleep` directly inside the fetch loop (3 attempts × 2 seconds). One failing message blocks every message behind it in the same partition for up to 6 seconds. With a prolonged transient failure (DB down), the whole consumer stalls and lag grows.

**Solution: retry topics.**
- Transient errors: publish to `<topic>.retry.5s`, `<topic>.retry.1m`, `<topic>.retry.10m` and commit immediately, without blocking the main partition.
- Only after the retry tiers are exhausted does the message go to `<topic>.DLQ`.
- Permanent errors (`Permanent()`) go straight to the DLQ as they do today.
- The DLQ has an alert (section 7) and a **redrive** tool (read the DLQ and publish back to the original topic after the bug is fixed).

Note on ordering: retry topics break ordering within a partition. For topics where ordering matters (`trip.status.changed`), the consumer must guard against reordering itself by comparing state (already present in Trip Service: state transitions are idempotent via `fromStatuses`).

### 3.4 The consumer exits the whole process on a fetch error

**Currently.** `kafkaconsumer.Run` calls `log.Fatalf` when `FetchMessage` returns an error, even when `ctx` has been cancelled (normal shutdown). This produces exit code 1 on shutdown, and there is no graceful shutdown yet.

**Solution.** When `ctx.Err() != nil`, leave the loop, commit the offset being processed, and close the reader and writer. For other fetch errors, retry with exponential backoff instead of ending the process. Each service's `main` catches `SIGTERM` (which Kubernetes sends during a rollout) using `signal.NotifyContext`.

### 3.5 Notification Service cannot scale horizontally

**Currently.** Every instance shares `GroupID: "notification-service-group"`, so Kafka **splits** the partitions among the instances. A rider connects their WebSocket to instance A, but their events may be read by instance B, and B does not hold that rider's socket, so the message is dropped. In addition, the `orderRider` and `driverRider` maps are in memory: a restart loses them, and an `order.matched` that arrives before the rider is known is ignored. Notification Service is also a point of silent information loss, because `pushToRider` returns when there is no socket and does not store the message.

**Solution (pick one).**

| Option | How | Trade-off |
|---|---|---|
| A. Fan-out via pub/sub | Notification reads Kafka in a shared group, looks up `riderId` in Redis, then `PUBLISH`es to the Redis Pub/Sub channel `rider:{id}`. Every WebSocket gateway subscribes to the channels of the sockets it holds. | Adds Redis Pub/Sub, but cleanly separates "event processing" from "holding sockets". **Recommended.** |
| B. One group per instance | `GroupID` includes the instance id; every instance reads every event and filters by the sockets it holds. | Simple, but every instance reads the entire traffic, which is expensive at scale. Stale groups accumulate. |
| C. Sticky by partition | Route each rider to the instance that holds their partition. | A rebalance drops connections; complex. |

In every option, the `orderId → riderId` and `driverId → riderId` mappings move from in-RAM maps to Redis (TTL of a few hours), or `riderId` is put directly into `order.matched` and `trip.status.changed` so the consumer does not need to look it up. The second approach is better: **events carry enough data for the reader** (see 4.2).

Remember: WebSocket is only a best-effort channel. Clients always have a recovery path via REST (design.md section 4d), and that remains the final guarantee.

### 3.6 Exactly-once: is it needed?

Do not use Kafka transactions (EOS) for the whole system. Reason: our side effects live outside Kafka (Postgres, Redis, WebSocket), so Kafka's EOS cannot cover them. The right, cheaper direction is **at-least-once + idempotent consumers + outbox**, which is already the project's model. Only consider Kafka transactions if there is a purely Kafka read-process-write segment (for example ksqlDB or Kafka Streams, which handle it themselves).

## 4. Kafka design

### 4.1 Topics, partitions, replication

| Topic | Key | Partitions (prod) | RF | Notes |
|---|---|---|---|---|
| `order.created` | `orderId` | 12 | 3 | Low load; 12 leaves room to scale Matching |
| `order.matched` | `orderId` | 12 | 3 | |
| `order.no_driver_found` | `orderId` | 6 | 3 | |
| `trip.status.changed` | `tripId` | 12 | 3 | |
| `trip.rated` | `driverId` | 6 | 3 | Keyed by driver so Rating can aggregate per driver |
| `driver.location.updated` | `driverId` | 48 | 3 (or 2) | Highest load, see 4.3 |
| `*.retry.*`, `*.DLQ` | same as source topic | 3 | 3 | |

How to choose the partition count: take the **maximum number of consumers you want to run in parallel** as the ceiling, then multiply by a headroom factor. Partitions cannot be reduced later, and increasing them changes the key → partition mapping (temporarily breaking ordering), so over-provision from the start.

The cluster has at least 3 brokers, spread across 3 availability zones, with `broker.rack` set so Kafka spreads replicas across zones. KRaft with 3 dedicated controllers (do not combine the controller and broker roles in production).

### 4.2 Event contract

- **Name events in the past tense, stating a fact that has happened** (`order.created`, `trip.status.changed`). Already the case in the project.
- **Events carry enough data** so the consumer does not have to call back into another service. For example, currently `order.matched` has no `riderId`, which forces Notification to remember the mapping in RAM. Adding `riderId` (a new field with an empty default, BACKWARD compatible) fixes this at the root.
- **Schema Registry** stays in `BACKWARD_TRANSITIVE` mode (checks against all previous versions, not just the immediately preceding one), because consumers may replay very old data.
- Register schemas in **CI/CD** (check compatibility before merge) instead of letting services register them at runtime. Turn off `auto.register.schemas` in production.
- Add `schemaVersion` or `traceId` fields to `Envelope` for tracing across services.

### 4.3 The location stream: the real bottleneck

Currently `driver-app-sim` publishes directly to Kafka because it is a simulation. In production, the driver app **never** talks to Kafka directly. A layer is needed:

```
Driver App ──HTTPS/gRPC/WebSocket──> Location Gateway ──batch──> [driver.location.updated]
```

- The gateway authenticates, rate-limits per driver, validates coordinates, and attaches a server-side `timestamp` in case the device lies.
- The producer sets `linger.ms=20-50` and a large `batch.size`, and enables compression, to pack many records into one request.
- The Driver Location Service consumer uses a **Redis pipeline** (or bulk `GEOADD`) instead of issuing one command per message, because Redis will become a hot spot before Kafka does.
- Updates can be **skipped** if the driver has barely moved (under 5m), which significantly reduces load.
- Redis GEO should be **sharded by region** (for example by geohash or by city) instead of a single `drivers:geo` key for the whole country. A sorted set that is too large makes `GEOSEARCH` progressively slower.

## 5. Data and state

| Data | Currently | Production |
|---|---|---|
| Orders, Trips, Ratings | One shared Postgres (the same `grabmini` database) | **One database per service** (or at least a separate schema/user) to be truly independent. Primary + replica, automatic failover, PITR backup. |
| Driver state and location | Single Redis | Redis Cluster or Sentinel. Losing locations on failover is acceptable because they recover on their own within a few seconds. **The ASSIGNED/ON_TRIP state** cannot be treated as ephemeral, so it needs a durable source of truth (Postgres), and Redis is only a cache that can be rebuilt from `trip.status.changed`. |
| `processed_events` table | Grows without bound | Clean up on a schedule (delete records older than the topic retention plus a safety margin). Partition by day for fast deletion. |
| Creating tables at startup | `CREATE TABLE IF NOT EXISTS` in `connectDB` | Use a migration tool (golang-migrate or goose) run in the deploy pipeline, not DDL at service startup (avoids races when several instances start together and avoids granting DDL privileges to the service). |

Because the source of truth for driver state should be durable, consider putting "driver is busy" into Trip Service's Postgres (a `driver_id` column + a partial unique index `WHERE status IN ('ASSIGNED','IN_PROGRESS')`) so that the DB itself guarantees "one driver, one active trip". Redis is then used only for spatial search.

## 6. Deployment and operations

### 6.1 Running on Kubernetes

- One `Deployment` per service, at least 2 replicas, a `PodDisruptionBudget`, spread across multiple zones (`topologySpreadConstraints`).
- **Autoscaling on consumer lag** (KEDA with the Kafka scaler) instead of CPU, because consumers wait on I/O. The number of replicas is capped by the number of partitions.
- Probes: `/health` currently only returns `ok`. Separate **liveness** (the process is alive) from **readiness** (can connect to Kafka, DB, Redis). A service that cannot reach its dependencies should not receive traffic.
- Rolling update with `maxUnavailable: 0`. Thanks to compatible schemas and idempotent consumers, old and new versions can safely run side by side.
- Rebalance: use the **cooperative sticky assignor** so scaling/rollouts do not stop the whole group. Set `group.instance.id` (static membership) so quick restarts do not trigger a rebalance.

### 6.2 Security

- Kafka: TLS between client and broker, **SASL/SCRAM or mTLS** authentication, **per-service ACLs** following least privilege (Order Service gets only `WRITE order.created` and `READ` on the two result topics). Currently everything is `PLAINTEXT` with no authentication.
- Schema Registry, ksqlDB, Kafka UI: behind authentication, **not exposed to the internet**. Kafka UI and ksqlDB currently expose their ports to everyone in dev.
- Public APIs: real authentication (JWT). Currently `riderId` is taken from the request body (`handleRateTrip` notes explicitly that there is no auth). Identity must be taken from the token, and the service must check that this rider actually owns the trip.
- WebSocket: authenticate when upgrading the connection and check `Origin` (currently `CheckOrigin` always returns `true`).
- Personal data (location, name): consider field-level encryption, and set a short retention on the location topic because it is sensitive data.
- Secrets (DB password, Kafka credentials) go through a secret manager, not default env values like the current `grabmini:grabmini`.

### 6.3 Backup and disaster recovery

- Kafka is not a backup. Retention is finite. For data that must be kept long term, dump it to object storage with the Kafka Connect S3 sink.
- MirrorMaker 2 replicates to a second cluster (another region) if multi-region DR is needed. Note that consumer offsets do not automatically line up between the two clusters.
- Postgres: continuous backup (WAL archiving) and periodic restore drills.
- Deliberate replay: resetting the offsets of a consumer group (like the rating experiment in the developer guide) is a legitimate recovery tool **because** consumers are idempotent. Write it up as a runbook.

## 7. Observability

**Required metrics** (Prometheus, via the Kafka exporter and each service's own metrics):

| Group | Metric | Alert when |
|---|---|---|
| Consumer | `consumer lag` per group and partition | lag keeps growing for more than 2 minutes |
| Consumer | handler error rate, retry count | error rate exceeds the threshold |
| DLQ | number of messages arriving in `*.DLQ` | **any** message on the transactional stream |
| Broker | under-replicated partitions, ISR shrink | non-zero |
| Producer | publish latency, error rate | p99 rises abnormally |
| Outbox | age of the oldest unsent record | over 30 seconds |
| Business | time from `order.created` to `order.matched`, `no_driver_found` rate | p95 above 3 seconds, or the rate spikes |

**Distributed tracing** (OpenTelemetry): propagate `traceId` through Kafka headers (`kafka.Message.Headers`), so a single order can be followed from HTTP to WebSocket across every service. **Structured logs** (JSON) with `orderId`, `tripId`, `eventId`, `topic`, `partition`, `offset` as standard fields, instead of free-form text `log.Printf` as today.

Suggested SLOs: 99.9% of orders created successfully, 95% of orders matched within 3 seconds (when drivers are available), 99% of trip status notifications reaching the client within 2 seconds.

## 8. Testing before production

- **Contract tests for schemas**: check in CI that each new `.avsc` version is compatible with the old one. There is already an example in `eventcodec_test.go`.
- **Integration tests with a real Kafka** (Testcontainers): currently the tests only use a fake Schema Registry, and there are no tests for the real publish and consume path.
- **Chaos testing**: kill a broker, kill a consumer mid-processing, slow down Redis, and check the invariants: no lost orders, no two trips for one order, no stuck driver, no double-counted rating.
- **Load testing**: simulate 25,000 msg/s of locations and 500 orders/s, and measure lag and match latency.

## 9. Proposed roadmap

Ordered by level of risk, not by technical appeal:

| Phase | Work | Why first |
|---|---|---|
| 1. Correctness | Outbox for Order/Trip (3.1); idempotency key and TTL for matching (3.2); graceful shutdown (3.4); `acks=all` | Events can currently be lost and drivers stuck, silently |
| 2. Fault tolerance | Retry topics (3.3); scalable notification (3.5); DB migrations; dependency health checks | Prevents cascading failures when one component is slow |
| 3. Security | User authentication, Kafka TLS/ACL, secret manager, Location Gateway | Prerequisite for exposing the system to the internet |
| 4. Operations | 3-broker multi-zone cluster, K8s, KEDA, metrics/traces/alerts, runbook | Needed before real traffic arrives |
| 5. Scale | Shard Redis GEO, S3 backup, multi-region DR, load testing | Do it when real numbers show it is needed |

## 10. What we deliberately do NOT do

- **Do not** use Kafka as a query database for the API. The current state lives in Postgres; Kafka is the transport and the log.
- **Do not** split into even smaller microservices. The six existing services already have enough boundaries; adding more only increases operational cost.
- **Do not** let clients connect to Kafka directly (keep the principle from design.md section 1).
- **Do not** enable exactly-once globally (see 3.6).
- **Do not** over-provision partitions "just to be safe": each partition costs file handles, broker memory and rebalance time.

## 11. Implementation status

### Phase 1 (done)

| Item | Result | Where in the code |
|---|---|---|
| 3.1 Outbox | Order Service and Trip Service store events in the `outbox` table in the same transaction as the state change (order, trip transition, rating). A relay publishes after the commit, with `Wake()` so it adds almost no latency. Each service relays only its own rows (`source`), and only one relay at a time (advisory lock), so per-key ordering is preserved. | `services/pkg/outbox`, `order-service/main.go` (`insertOrder`), `trip-service/main.go` (`transitionWithEvent`, `rateWithEvent`) |
| 3.2 Matching | `match:{orderId}` records the match decision at the same time as the claim (a single Lua script). If `order.created` is redelivered, the previous driver is reused. The `ASSIGNED` state is a **lease** (default 120 seconds, `ASSIGN_LEASE_SECONDS`) rather than a key TTL: once it expires, the driver can be claimed again. | `pkg/driverstatus` (`ClaimForOrder`), `matching-service/main.go` |
| 3.4 Shutdown | `kafkaconsumer.Run` exits when the context is cancelled, commits messages that have finished processing, does not commit a message that is mid-flight, and no longer calls `log.Fatalf` on a fetch error (backs off and retries). Every service catches SIGTERM. | `pkg/kafkaconsumer`, `pkg/serve`, each service's `main.go` |
| `acks=all` | Every writer on the transactional stream (the outbox relay of Order/Trip, Matching, DLQ) uses `RequireAll`. Driver locations use `RequireOne`. | |

Behavior changes to be aware of:

- **`ON_TRIP` starts when the trip is created.** Previously the driver stayed `ASSIGNED` for the whole ride. Because `ASSIGNED` is now a lease, Trip Service sets `ON_TRIP` (and ends the lease) when it creates the trip from `order.matched`.
- **Trip Service creates the trip and writes `processed_events` in a single transaction.** Previously the dedupe record was written before the trip was created, so if creating the trip failed, the retry was skipped and the trip was never created.
- A create-order request returns `201` as soon as the transaction commits, even when Kafka is down. The event is sent after Kafka comes back.

### Releasing the driver when the trip ends (done)

Previously, setting the driver back to `AVAILABLE` happened inside the request, outside the transaction: if the process died between committing the trip state and the Redis command, the driver was stuck `ON_TRIP`, and the lease could not rescue it. Now Trip Service also consumes `trip.status.changed` (group `trip-driver-release-group`) and releases the driver when the trip is `COMPLETED` or `CANCELLED`. The event is enqueued in the same transaction as the state, so it cannot be lost. The in-request path is kept as a fast path.

The consumer checks the current state rather than trusting the event, because events can arrive late or be replayed:

- if the driver still has an active trip in the DB, leave them as they are;
- the release is a compare-and-set `ON_TRIP` → `AVAILABLE` (`driverstatus.Transition`), so if Matching has just claimed the driver for a new order (the DB has no trip yet), that claim is not overwritten.

Code: `trip-service/consumer.go` (`releaseDriverAfterTrip`), `pkg/driverstatus` (`Transition`).

### Known limitations of phase 1

- **The relay is at-least-once**: if it publishes and then dies before deleting the row, the event goes out twice. Consumers already dedupe by `eventId` or are idempotent by state.
- The `outbox` table shares one Postgres between the two services in the local version; once databases are split per service (section 5), each service has its own table.
- Matching still publishes directly (it has no DB to use an outbox with). The gap between claim and publish is closed by the match record and the lease, not by an outbox.
- The Kafka parts (relay, consumer) were tested against a real broker in phase 2; at the time of writing the whole system had not yet been run end-to-end (see the testing section below; this has since been done).

### How it was tested

The automated tests run on Postgres 16, Redis 7 and **a real Kafka 3.8** (KRaft, single node): the relay (rollback, ordering, publish failure, multiple relays, split by source, publishing through a real `kafka.Writer`), the lease and per-order claim (including 10 concurrent claims), matching (redelivery, not yet announced, publish failure), Trip Service (trip creation, replay not flipping driver state, transition and rating in the same event, driver release), and the consumer (retry through the tiers, DLQ, permanent errors, not blocking the partition, shutdown, a message interrupted midway being redelivered). These tests skip themselves if `TEST_DATABASE_URL`, `TEST_REDIS_ADDR`, `TEST_KAFKA_BROKERS` are not set (see the [developer guide](../../docs/guide.md)).

**Status when this was written:** the whole system had not been run (Schema Registry, ksqlDB, the React apps) because the development environment had no Docker daemon. **Since then** `pnpm dev:all` has run the full stack locally (a ride was booked, driven and rated through the apps), and the `e2e-test` job in CI runs the services against real Kafka, Postgres and Redis. The Kubernetes manifests, KEDA scaling and the Sentinel/ACL deployment have still not been run on a real cluster.

## 12. Phase 2 (done)

| Item | Result | Where in the code |
|---|---|---|
| 3.3 Retry topics | With `RetryTiers`, a transient error sends the message to `<topic>.retry.1`, `.2`, `.3` (waiting 5 seconds, 1 minute, 10 minutes) and the partition keeps moving. Each tier has its own reader (group `<group>.retry.<n>`) that waits until the message is due and then retries; after the last tier it goes to the DLQ. Permanent errors still go straight to the DLQ. The handler always sees the original `msg.Topic`. Retry topics are created at startup if they do not exist. Order, Matching, Trip and Rating enable `DefaultRetryTiers`. | `pkg/kafkaconsumer` |
| 3.5 Notification horizontal scaling | Events carry `riderId` (`order.matched`, `order.no_driver_found`, `trip.status.changed`; the new field has default `""`, so it is BACKWARD compatible). Notification does not write to sockets directly from a Kafka event: it `PUBLISH`es to the Redis channel `notify:rider:<id>` / `notify:driver:<id>`, and whichever instance holds the socket (having `SUBSCRIBE`d at connect time) writes it. Routing state (order→rider for old events, driver→rider for locations) lives in Redis. | `notification-service/hub.go`, `consumer.go`; `matching-service`, `trip-service` (column `trips.rider_id`) |
| DB migrations | Each service has `migrations/*.sql` embedded in the binary, run by goose, with its own version table (`schema_migrations_<service>`) and an advisory lock so multiple instances do not run it twice. `<service> migrate` runs and exits; `RUN_MIGRATIONS=false` skips it at startup. | `pkg/migrate`, `*/migrations/` |
| Health checks | `/livez` (always 200) is separate from `/readyz` (checks Postgres/Redis/Kafka, each check with a 2-second timeout, run in parallel, 503 naming the failing dependency). `/readyz` returns 503 immediately on SIGTERM so the load balancer drains traffic first. Matching and driver-location have no HTTP API, so they only expose probes when `HEALTH_PORT` is set. The old `/health` is kept as is (liveness) for `dev.sh`. | `pkg/health` |

Findings from testing on a real Kafka:

- **`kafka.Reader` defaults `MaxWait` to 10 seconds, and `Close()` waits for the pending fetch.** Services took 6 to 10 seconds to stop, even with the phase 1 graceful shutdown. With `MaxWait` set to 500ms, `Close()` takes a few milliseconds.
- A `Reader` with a `GroupID` does not create a missing topic by itself, so `kafkaconsumer` creates the retry topics at startup.

Behavior changes and trade-offs to be aware of:

- **Retry topics reorder messages**: a message is retried after the messages that came behind it. The handlers that enable `RetryTiers` already tolerate this (state transitions are guarded by the current state, and every event has an `eventId`). `driver.location.updated` and Notification do not enable tiered retry: a notification re-sent minutes later would only show stale state.
- **Redis becomes a dependency of Notification Service** (previously only Kafka). A message published to a Redis channel while nobody is subscribed is dropped, which matches the best-effort semantics of WebSocket: the app recovers through REST after reconnecting (design.md section 4d).
- Each location update costs one extra Redis `GET` (the driver→rider lookup) and at most two `PUBLISH`es. At 25,000 msg/s (section 1) this is the place to measure; if it becomes a problem, add a local cache for the driver→rider mapping.
- Trip Service stores `rider_id` on the trip; trips created before this change have an empty `rider_id`, so their events use the fallback path (order→rider lookup in Redis).

## 13. Phase 3 (done)

| Item | Result | Where in the code |
|---|---|---|
| User authentication (6.2) | Every Order, Trip and Rating route requires a JWT (EdDSA/Ed25519 with a `kid`: services hold only the public keys, fetched from the issuer's JWKS; one algorithm pinned; issuer, audience and expiry checked; see `docs/security.md`). Identity comes from the token, not the body: a body that names a `riderId` different from the token gets a 403. Ownership checks: a rider sees only their own orders and trips (others get a 404 as if it did not exist); only the trip's driver can pickup and complete; only the trip's rider can rate; both sides can cancel. | `pkg/auth`, `*/main.go` (`routes`) |
| Token issuer | `auth-service` (port 3005): only a **dev identity-provider stand-in**, which issues a token to anyone who asks. It refuses to start when `APP_ENV=production` unless `ALLOW_DEV_TOKENS=true` is set. Production must replace it with a real IdP. | `services/auth-service` |
| WebSocket (6.2) | The token travels in a subprotocol (`bearer, <token>`), not in the query string, so it does not end up in access logs. The socket for rider X or driver X can only be opened with a token of the right role and the right id. `Origin` is checked against `ALLOWED_ORIGINS` (mandatory in production). | `notification-service/main.go` |
| Frontend | `@grab-mini/client` logs in, attaches the token to every request (retrying once on 401) and to the WebSocket. | `packages/client/src/auth.ts` |
| Secret manager (6.2) | `NAME_FILE` or `NAME`; dev defaults are used only outside production. Applies to `DATABASE_URL`, `JWT_PRIVATE_KEY` (issuer only), `KAFKA_PASSWORD`, `SCHEMA_REGISTRY_PASSWORD`. | `pkg/secret` |
| Location Gateway (4.3) | The driver app never talks to Kafka. The gateway (port 3006) authenticates the driver, validates coordinates, limits each driver to 1 report per second (429 with `Retry-After`), drops reports from a stationary driver (under 5 m) except one heartbeat every 30 seconds, drops reports whose device timestamp is more than 30 seconds off the server clock, and publishes in 20ms batches with lz4 and `acks=1`. `driver-app-sim` now calls the gateway over HTTP like a real client. | `services/location-gateway` |
| TLS + SASL/SCRAM + ACL for Kafka (6.2) | `pkg/kafkaconn` enables TLS and SASL through environment variables (`KAFKA_SECURITY_PROTOCOL`, `KAFKA_USERNAME`, `KAFKA_PASSWORD[_FILE]`, `KAFKA_CA_FILE`); production refuses plaintext connections unless `KAFKA_ALLOW_PLAINTEXT=true` is set. One principal per service; `scripts/kafka-acls.sh` grants exactly the right permissions (write the topics it produces, read the topics it consumes, and Read/Write/Describe/Create on the `<topic>.` prefix for its own retry topics and DLQ). `deploy/kafka-secure/start-local.sh` brings up a single-node broker with all three security layers for trying it on a laptop. | `pkg/kafkaconn`, `scripts/kafka-acls.sh`, `deploy/kafka-secure/` |

Verified on a real broker with TLS, SASL/SCRAM, ACLs and `auto.create.topics.enable=false` (brought up with `start-local.sh` itself): the matching-service consumer ran the full failure path (retry topics, DLQ) with exactly its own permissions; order-service can write to `order.created` but is denied when writing to `order.matched`; rating-service cannot read `order.created`; a wrong password, an unknown user and an untrusted broker certificate are all rejected; `trip-service` ran with its own principal and `/readyz` returned 200.

Findings along the way:

- `auto.create.topics.enable=false` is the correct production setting, but the DLQ used to exist only thanks to the broker's auto-create. `kafkaconsumer` now creates the DLQ itself as well (using the Create permission on the `<topic>.` prefix), as it already did for retry topics.
- The prefix ACL `order.created.` matches `order.created.retry.1` and `order.created.DLQ` but does **not** match `order.created` itself, so permissions on the original topic and on the auxiliary topics are kept separate.

Known limitations, not done:

- **The token is checked only when the WebSocket is opened.** An open connection stays alive after the token expires (1 hour). The socket needs to be closed when the token expires, or the client needs to be required to send a new token.
- ~~HS256 uses a shared key~~ done: tokens are EdDSA, services hold only the public keys (JWKS), and HS256 is no longer accepted.
- **The gateway's rate limit lives in each instance's RAM.** Running several instances without sticky routing lets a driver report faster than the limit (each instance allows one). Redis or driver-sticky routing is needed when scaling.
- **A driver can read any order if they know its UUID** (needed so the Driver App can fetch the pickup point). UUIDs are hard to guess, but it would be tighter if Order Service stored the matched driver (having received `order.matched`) and let only that driver read it; I haven't done this because there is a race between writing `driver_id` and the Driver App fetching the order right after receiving the notification.
- **Schema Registry, ksqlDB and Kafka UI still have no authentication** and still expose their ports in `docker-compose.yml` (section 6.2 of this phase). Services talk to Schema Registry over unauthenticated HTTP. `docker-compose.yml` still runs Kafka PLAINTEXT for dev. Services now support basic auth and https to Schema Registry (`SCHEMA_REGISTRY_USER`/`PASSWORD`, `SCHEMA_REGISTRY_CA_FILE`) and cap each registry response at 1 MiB, but the registry in `docker-compose.yml` is still open.
- Redis and Postgres do not have TLS enabled, and Redis has no password (only `DATABASE_URL` goes through `pkg/secret`).
- There is no mTLS between services, no field-level encryption of personal data, and no short retention set on the location topic.
- The whole system has not been run end-to-end (Schema Registry, ksqlDB, the apps) because the sandbox has no Docker. The gateway and the authenticated routes were tested with automated tests and with real binaries against Postgres, Redis and Kafka; the browser flow (login, WebSocket with the subprotocol) has only been checked by typecheck and has not been run in a browser.

## 14. Phase 4 (done)

| Item | Result | Where in the code |
|---|---|---|
| Structured logging (7) | `logging.Setup` routes every log line, including `log.Printf`, through `slog`: JSON with `service` in production. A line inside a span additionally gets `trace_id` and `span_id`. The main consumer lines have `topic`, `partition`, `offset`, `key` as separate fields. | `pkg/logging`, `pkg/kafkaconsumer` |
| Metrics (7) | `/metrics` on every service: HTTP by route (not by URL), publishes by topic, `kafka_consumer_messages_total{result=ok\|retried\|dead_lettered}`, `outbox_oldest_pending_age_seconds`, match latency (3-second SLO), orders created, trip state transitions, locations by outcome, open sockets. | `pkg/metrics`, each service |
| Tracing (7) | W3C `traceparent` across HTTP and Kafka headers, including through the outbox (stored alongside the row, in the `headers` column) and through retry topics. A request that passes through Postgres, a real Kafka and a retry topic is **one trace**, and every consumer span is a child of the request span. Exports OTLP when `OTEL_EXPORTER_OTLP_ENDPOINT` is set; sampling is 100% (dev) or 10% (production). | `pkg/tracing`, `pkg/outbox` |
| Admin port (6.1) | `HEALTH_PORT` moves `/livez`, `/readyz`, `/metrics` to a separate port and removes them from the public mux: an ingress cannot expose `/metrics`. | `pkg/health` (`Mount`) |
| Deployment (6.1) | Dockerfile (distroless, non-root); a Kustomize base generated by `generate.py`: Deployment (non-root, read-only filesystem, probes, `maxUnavailable: 0`, spread across zones, PDB, `preStop`), migration Job, KEDA scaling on lag (capped by the partition count), NetworkPolicy, PodMonitor. | `deploy/` |
| Kafka cluster (4.1) | Strimzi: 3 KRaft controllers and 3 brokers spread across 3 zones, `replication.factor=3`, `min.insync.replicas=2`, no unclean leader election, no topic auto-creation. `KafkaUser` and ACLs are **read from `scripts/kafka-acls.sh`**: a single table of permissions. | `deploy/k8s/base/kafka/` |
| Alerts (7) | 15 rules: dead letters (page on any message), growing lag, error rate, URP, missing brokers, publish failures, stuck outbox, quarantined rows, the 3-second match SLO, no-driver rate, multi-window burn rate for booking. Each rule has a "must fire" and a "must not fire" test case run with `promtool test rules`. | `deploy/monitoring/` |
| Runbook | One entry per alert: what to check, in what order, what to do; plus deploy, replay and secret rotation. | `docs/runbook.md` |
| DLQ redrive (3.3) | `dlq-redrive` pushes dead letters back to the original topic with the exact key and bytes; remembers its position in a consumer group; `-dry-run`, `-error`, `-max`. | `services/dlq-redrive` |

Bugs found and fixed while doing this phase:

- **`metrics.HTTP` broke WebSocket.** Its response writer hid `http.Hijacker`, so no socket could be opened through the middleware.
  Found when running Notification Service's tests through the exact production middleware chain. (A seemingly
  harmless mid-way change would have broken Notification Service completely.)
- **The outbox relay was blocked by one bad event.** An event Kafka would never accept (for example, one that is too large) made every flush fail:
  later events got stuck and the good events in the same batch were resent forever. Now a failing batch is tried event by event; an event
  that is permanently rejected is moved to `outbox_dead` (with a metric and an alert), transient errors are kept and the part already sent is committed.
- **Message loss when the retry topic and the DLQ both refuse the write.** The consumer still committed that message. Now it holds the partition and keeps trying until
  the message is safe on one of the two (or stops on shutdown, so the message is redelivered).
- DLQs and retry topics are created with `KAFKA_REPLICATION_FACTOR` (production: 3) instead of always 1.

Known limitations of this phase:

- **Not run on a real cluster.** The manifests passed kubeconform (strict, with the CRD schemas for Strimzi, KEDA and Prometheus Operator),
  cross-checking against the code, and `promtool`. They have not been applied to a cluster, so how Strimzi/KEDA actually behave, the
  RBAC permissions, zone-aware PVCs, and the Dockerfile (not built because the sandbox has no Docker daemon) are all unverified.
- Alert thresholds are estimates based on the assumptions in section 1, not calibrated against real traffic.
- There are no Grafana dashboards, and no Postgres/Redis/Schema Registry/OpenTelemetry collector in the manifests (set through `config.yaml`).
- There is no static membership (`group.instance.id`) for consumers: `kafka-go` v0.4 does not support it, so every pod restart still causes a rebalance.
- Every Deployment uses a shared ServiceAccount and there is no egress NetworkPolicy.
- Matching and Location Gateway still have no outbox (see the runbook).

## 15. Phase 5 (partly done)

The principle of this phase is **measure first, optimize after**, so most of the output is the measurement tooling and what it found.

- **Kiểm thử tải** (`deploy/loadtest`, `services/loadtest`, `services/dev-registry`): chạy các service thật không cần Docker; sinh tải mở
  (đặt đơn + tài xế báo vị trí qua gateway), đọc quantile từ `/metrics`. Tìm ra và sửa: `BatchTimeout` 1 s mặc định của kafka-go (mọi publish
  đồng bộ ≥ 1 s), commit đồng bộ từng message (`CommitInterval`), consumer tuần tự (`Workers`, một worker mỗi partition vẫn giữ thứ tự),
  và tối đa 50 round trip Redis mỗi đơn khi tài xế gần đã bận (`ClaimFirstForOrder`, một script). Kết quả và diễn giải trung thực:
  [docs/performance.md](../../docs/performance.md): 250 đơn/s + 750 vị trí/s sạch trên một máy 4 core; 400 đơn/s thì máy bão hòa.
- **Redis GEO**: đã đo. Chi phí tìm kiếm tỉ lệ với số ứng viên trong bán kính, không phải tổng tài xế; đã làm tìm theo vòng mở rộng
  (1 → 3 → bán kính cấu hình). **Shard theo ô có sẵn nhưng tắt** (`GEO_SHARD_DEGREES`, mặc định 0): đã đo, nó không làm mỗi truy vấn
  rẻ hơn mà chỉ chia tải ra nhiều key, nên chỉ bật khi các key nằm trên nhiều node; chưa dùng được với Redis Cluster (xem docs/performance.md).
- **Dọn dữ liệu**: CronJob xóa `processed_events` và `rating_events` cũ hơn 30 ngày theo lô (đã thử trên Postgres thật); `outbox` đã tự
  xóa khi publish, `outbox_dead` để người xử lý.
- **Lưu trữ đơn và chuyến đi**: `retention-order-orders` và `retention-trip-trips` (CronJob hằng ngày, `scripts/pg-retention.sh`) xóa theo lô 5.000 đơn
  `CANCELLED`/`NO_DRIVER`, chuyến `COMPLETED`/`CANCELLED` và marker `cancelled_orders` cũ hơn 180 ngày (cấu hình được, tối thiểu 30); đơn `MATCHED` (trạng thái cuối order-service biết) sau 365 ngày;
  không bao giờ đụng `PENDING`, chuyến đang chạy, `outbox`, `outbox_dead`, `driver_reviews`. Đã chạy trên Postgres thật với migration thật
  (`scripts/test-data-ops.sh`: 12.009 đơn và 8 chuyến đủ trạng thái/độ tuổi, kiểm từng dòng còn lại). Xóa là mất dữ liệu ở DB sống; kho lưu trữ dài hạn là bản sao lưu.
- **Redis trong manifest**: `deploy/k8s/base/redis/`, StatefulSet 3 pod (redis + sentinel + exporter), AOF, PVC, PDB, anti-affinity theo node và zone, mật khẩu từ Secret, PodMonitor, cảnh báo;
  service trỏ vào Sentinel qua ConfigMap. `scripts/test-redis-ha.sh` chạy đúng các script đó trên Redis/Sentinel thật: dựng cụm, kill master (failover 4 đến 5 s, không mất key), pod cũ vào lại làm replica,
  `preStop` chuyển master, và `TestTheClientFollowsAFailover` pass trên bộ 3 Sentinel.
- **Sao lưu và diễn tập khôi phục Postgres**: CronJob `backup-<service>` (6 giờ một lần, `pg_dump -Fc` lên S3 tương thích, credential từ Secret) và `scripts/pg-restore-drill.sh`
  (khôi phục bản mới nhất vào DB scratch, kiểm tra phiên bản schema, bảng, số dòng). Đã chạy trọn vẹn trên Postgres thật, kể cả đường S3 và các trường hợp drill phải fail.
- **Sao lưu và DR**: [docs/disaster-recovery.md](../../docs/disaster-recovery.md) (dữ liệu nào quan trọng đến đâu, RPO/RTO đã đo và chưa đo, quy trình chuyển vùng),
  `deploy/k8s/dr/mirrormaker2.yaml` và `s3-archive.yaml` (kiểm tra schema bằng `deploy/verify.sh`, **chưa chạy trên cluster thật**).

Giới hạn còn lại: chưa có số đo trên hạ tầng thật (mục tiêu 500 đơn/s và 25.000 vị trí/s chưa kiểm chứng); **chưa diễn tập chuyển vùng**; manifest Redis, sao lưu và retention
mới được kiểm tra schema + kiểm tra chéo và chạy các script của chúng trên Docker, **chưa chạy bởi kubelet trên cluster thật** (lập lịch theo zone, PVC, NetworkPolicy cho Redis/egress, S3 thật);
RTO khôi phục Postgres chưa đo ở kích thước production (drill cục bộ chỉ ~8 KB); sao lưu Postgres là dump 6 giờ một lần, không PITR (cần Postgres managed); Redis Cluster chưa hỗ trợ
(script dựng key động); Redis vẫn một master (Sentinel cho sẵn sàng chứ không cho dung lượng), và hệ quả của việc mất vài giây ghi cuối khi failover (lease, `match:<orderId>`) chưa được kiểm thử.
Cùng với các giới hạn ở mục 13 và 14 và việc nhỏ từ giai đoạn 2 (cache ánh xạ tài xế→rider nếu đo thấy cần).

## 16. Thanh toán: Payment Service (đã làm)

Chuyến đi giờ được tính tiền. Không có cổng thanh toán thật: `PaymentProvider` là một interface (`services/payment-service/provider.go`)
và chỉ có bản `fake` (sandbox, rider có id bắt đầu bằng `decline-` bị từ chối). Service **không bao giờ** nhận số thẻ: `ChargeRequest`
chỉ có `RiderID`, phương thức thanh toán nằm ở phía nhà cung cấp.

**Giá đến Payment Service bằng cách nào.** Đã chọn: tiêu thụ `order.created` và giữ bản sao cục bộ `order_prices(order_id, rider_id, price)`
(giá đã khóa lúc đặt, v3 của schema). Phương án còn lại là thêm trường `price` (có default) vào `trip.status.changed`, nhưng Trip Service
không biết giá: phải mở rộng cả `order.matched` (Matching) và Trip Service, tức sửa ba service và ba schema chỉ để chở một số mà
Order Service đã công bố. Chọn bản sao cục bộ thì **không đổi schema nào**, không đổi service nào khác, và Payment Service không phụ thuộc
một lời gọi đồng bộ sang Order Service. Cái giá: hai topic khác nhau nên không có thứ tự, `trip.status.changed` có thể đến trước
`order.created`. Khi đó handler trả lỗi tạm thời và event đi qua các tầng retry (5s, 1 phút, 10 phút); chỉ khi `order.created` thật sự
không bao giờ đến thì nó vào DLQ và cảnh báo `PaymentDeadLettered` page. Đơn tạo trước khi có giá (`price` null) không bị tính tiền
(`payments_skipped_total{reason="no_price"}`). Chỉ schema mới `payment.recorded` được thêm; mọi trường sau eventId/occurredAt/định danh
đều có default, nên tiến hóa về sau vẫn BACKWARD compatible.

**Quy tắc tiền.**
- Cước: đúng giá đã khóa, tính khi `trip.status.changed` = COMPLETED.
- Tip: `trip.rated.tipAmount`, một khoản tính riêng (`TIP`), tài xế nhận đủ, không chiết khấu.
- Huỷ: huỷ khi đơn còn PENDING thì không có chuyến và không có event nào Payment Service đọc (nó không đọc `order.cancelled`), nên không
  bao giờ bị tính. Rider huỷ **sau khi** có tài xế (`trip.status.changed` CANCELLED, `cancelledBy=RIDER`) chịu phí `CANCELLATION_FEE_VND`,
  **mặc định 0** để giữ nguyên hành vi hiện tại (huỷ vẫn miễn phí; bật bằng cấu hình), tối đa bằng giá chuyến. Tài xế huỷ, hoặc
  `cancelledBy` rỗng (event cũ), thì không tính.
- Chiết khấu nền tảng: `PLATFORM_COMMISSION_BPS`, mặc định 2000 (20%) cho cước và phí huỷ; làm tròn xuống, tài xế nhận phần còn lại, nên
  không một đồng nào sinh ra hay mất đi.

**Sổ cái.** Bút toán kép trong Postgres: tài khoản `RIDER`, `DRIVER_PAYABLE`, `PLATFORM_REVENUE`; mỗi giao dịch là các bút toán có dấu
(âm = trừ khỏi tài khoản) cộng đúng bằng 0. Tiền là `BIGINT` đơn vị nhỏ nhất (VND không có đơn vị nhỏ hơn đồng), không có float nào sau
chỗ đổi `price` (float64 của event) sang số nguyên. Cân bằng được kiểm **hai lần**: trong code (`Txn.Validate`, trước khi gọi nhà cung cấp
và trước khi ghi; vi phạm đếm vào `ledger_unbalanced_rejected_total`) và trong DB (constraint trigger hoãn đến lúc commit); sổ cái chỉ
được thêm (trigger chặn UPDATE/DELETE). Số dư là tổng bút toán, không lưu riêng. Mỗi khoản duy nhất theo `(trip_id, kind)`;
event dedupe theo `eventId` trong **cùng transaction** với `payments`, bút toán và dòng outbox của `payment.recorded`. Gọi nhà cung cấp
nằm ngoài transaction và idempotent theo khóa (`fare:<tripId>`), nên sập giữa chừng rồi thử lại không tính hai lần.
Từ chối thẻ (`DECLINED`) được ghi và công bố, không có bút toán, không tự thử lại.

**Giới hạn đã biết:** nhà cung cấp là giả; chưa có chi trả thật cho tài xế (số dư `DRIVER_PAYABLE` chỉ ghi nhận), chưa hoàn tiền/đảo bút toán,
chưa xử lý nợ khi thẻ bị từ chối, chưa đối soát với báo cáo của nhà cung cấp. Egress cần cho Payment Service: Postgres, Kafka, Schema
Registry, OTLP collector (không cần Redis); khi có nhà cung cấp thật thì thêm HTTPS tới nhà cung cấp đó.
