# Developer guide

How to run, explore and operate grab-mini. For what it is and why it is built this way, start at the [README](../README.md) and the [architecture decision records](adr/README.md).

A rider books a ride, a matching service finds the nearest driver, a trip runs through its lifecycle, and both apps update in real time. Every step between services is a Kafka event, serialized as Avro and validated against a schema in Schema Registry. ksqlDB reads the same topics to compute live analytics.

```
Rider App ──POST──> Order Service ──> [order.created] ──> Matching Service ──> [order.matched] ──┬──> Trip Service ──> [trip.status.changed] ──┐
                                                                     └──────> [order.no_driver_found]  │                                    │
Driver App (simulated) ──> [driver.location.updated] ──> Driver Location Service ──> Redis GEO        └────────────────────────────────────┴──> Notification Service ──WebSocket──> Rider / Driver App
```

The requirements, design and task breakdown live in [specs/ride-booking-mini](../specs/ride-booking-mini).

## Quick start

**Prerequisites:** Docker Desktop (running), Go 1.26+, Node 24 (22.22+ also works; Node 20 is too old for the test toolchain), and pnpm 9 (`corepack enable`, or `npm i -g pnpm`).

```bash
pnpm install
pnpm dev:all
```

`pnpm dev:all` (or `make dev`, which runs [scripts/dev.sh](../scripts/dev.sh)) starts the Docker infra (Kafka, Schema Registry, ksqlDB, Postgres, Redis, Kafka UI, Prometheus, Loki, Promtail, Grafana), waits for it to be healthy, seeds five drivers, builds and runs every Go service plus the driver simulator, and starts the apps. Run `make help` to see all automation targets. When it prints `grab-mini is up`:

| What | URL |
|---|---|
| Rider App | http://localhost:5171 |
| Driver App | http://localhost:5172/?driver=driver-1 |
| Ops dashboard | http://localhost:5173 |
| Grafana (Metrics & Logs) | http://localhost:3000 |
| Prometheus | http://localhost:9090 |
| Loki | http://localhost:3100/ready |
| Kafka UI | http://localhost:8080 |
| Schema Registry | http://localhost:8081/subjects |
| ksqlDB | http://localhost:8088/info |

Ctrl-C stops the services and apps (the Docker infra keeps running). Logs are in `.dev/logs/<name>.log` and shipped by Promtail to Loki so you can query them in Grafana (`{job="grab-mini"}`). Use `make dev-reset` (`scripts/dev.sh --reset`) to wipe data first, and `make infra-down` (`docker compose down`) to stop the infra.

### Take a ride

1. In the **Rider App**, pick **Car** or **Bike**, tap the map to drop a destination pin, then **Book ride**.
2. The rider is matched with the nearest driver of that type and can watch them move on the map. Five drivers are seeded around central Ho Chi Minh City: `driver-1` to `driver-3` drive cars, `driver-4` and `driver-5` drive bikes.
3. Open the **Driver App as that driver**, e.g. `http://localhost:5172/?driver=driver-2`. **Confirm pickup**, then **Complete trip**, and the rider's timeline follows along.
4. Back in the **Rider App**, rate the driver (1 to 5 stars) and submit. You can also add a comment and a tip (0, 10k, 20k, 50k or custom VND). A tip is only recorded, there is no payment system: Rating Service keeps a per-driver total and the driver's app shows it in the toast. The driver's app shows a toast and their updated average.

Also try: rating a trip twice (the second is rejected), booking a bike (only `driver-4`/`driver-5` can take it), **Cancel trip** from either side, or booking when every driver is busy (you get "No drivers nearby" after the matching service's retries). `?rider=rider-2` picks a different rider.

The map background comes from OpenStreetMap tiles, so it needs internet access; markers work without it.

## Watching Kafka work

Open [Kafka UI](http://localhost:8080), cluster **grab-mini**, while you take a ride:

- **Topics**: `order.created`, `order.matched`, `order.no_driver_found`, `trip.status.changed` and `driver.location.updated`, each with 3 partitions. Open a topic's **Messages** tab: Kafka UI decodes the Avro values through Schema Registry, so you see JSON. `driver.location.updated` grows every few seconds.
- **Partition keys**: messages for one order (or one driver) always land in the same partition, which keeps their order. Compare the **Partition** column across messages.
- **Schema Registry** (menu item in Kafka UI, or `curl localhost:8081/subjects`): one subject per topic (`order.created-value`, ...), each holding the Avro schema producers registered.
- **Consumers**: one consumer group per service (`matching-service-group`, `trip-service-group`, `driver-location-service-group`, `notification-service-group`). Each reads every event on its topics independently. Check **lag** here: it stays at or near 0 (a message or two in flight on `driver.location.updated` while the simulator publishes; `N/A` just means a group hasn't committed an offset on that topic yet).

## Analytics with ksqlDB

The [Ops dashboard](http://localhost:5173) shows live figures (orders per minute by vehicle type, match rate, average match time, trips completed, driver ratings, location update throughput). None of it is computed by a service we wrote: [ksqlDB](https://ksqldb.io) reads the Avro topics and maintains the tables in [ksql/statements.sql](../ksql/statements.sql), and the dashboard just runs pull queries against them (through the ksqlDB REST API).

| Table | Built from | What it shows |
|---|---|---|
| `orders_per_minute` | `order.created` | orders per vehicle type, in one-minute tumbling windows |
| `match_stats` | join of `order.created` and `order.matched` | matches, average and max time to find a driver, by vehicle type |
| `matched_total`, `no_driver_total` | `order.matched`, `order.no_driver_found` | how many orders found a driver |
| `trip_status_totals` | `trip.status.changed` | trips by status (completed versus cancelled) |
| `driver_rating_stats` | `trip.rated` | average stars per driver |
| `location_updates_per_minute` | `driver.location.updated` | throughput of the busiest topic |

`driver_rating_stats` computes the same average as Rating Service does in Postgres: one is a declarative query over the topic, the other a consumer we wrote and deduplicate ourselves. Compare them, and note what ksqlDB gives you for free (no consumer code, replayable from the topic) versus what it doesn't (the result lives in ksqlDB, not in a database the other services read).

Try it yourself, from the ksqlDB CLI (or the **KSQL** section of Kafka UI):

```bash
docker run --rm -it --network grab-mini_default confluentinc/ksqldb-cli:0.29.0 ksql http://ksqldb-server:8088
```
```sql
SELECT * FROM match_stats;                                   -- a pull query: the current value
SELECT ORDERID, VEHICLETYPE FROM orders_created EMIT CHANGES;  -- a push query: streams new orders as they arrive
SHOW QUERIES;                                                -- every table above is a running query
```

ksqlDB infers column names from the Avro schemas and upper-cases them (`orderId` becomes `ORDERID`). The tables are rebuilt from the Kafka topics, so they survive `docker compose down`.

## Things to try

- **Scale a consumer group.** Start a second matching service in another terminal and watch Kafka split the 3 partitions between the two instances, with no order matched twice:
  ```bash
  cd services/matching-service && INSTANCE_ID=matching-B go run .
  ```
  The **Consumers** tab in Kafka UI shows the partition assignment change.
- **Send a message that isn't Avro** and watch it land in a dead-letter topic. A hand-published JSON message has no Schema Registry header, so the Driver Location Service can't decode it. Decoding errors are permanent, so it skips the retries and publishes straight to `driver.location.updated.DLQ`, with the original bytes (`valueBase64`, plus a readable `valueText`) and the error attached. (Transient failures, like Redis being down, are retried 3 times, 2s apart, before dead-lettering.)
  ```bash
  docker exec grab-mini-kafka bash -c 'echo "{\"driverId\":\"driver-99\"}" | /opt/kafka/bin/kafka-console-producer.sh --topic driver.location.updated --bootstrap-server localhost:9092'
  docker exec grab-mini-kafka /opt/kafka/bin/kafka-console-consumer.sh --topic driver.location.updated.DLQ --bootstrap-server localhost:9092 --from-beginning
  ```
- **Read about a schema that evolved.** Vehicle types were added after the first release by giving `order.created` and `order.matched` a `vehicleType` field with a default of `CAR`, which is a backward-compatible change. A fresh start registers only the current version (`curl localhost:8081/subjects/order.created-value/versions`), but the test `TestCurrentOrderCreatedReadsMessagesWrittenBeforeVehicleType` in [services/pkg/eventcodec](../services/pkg/eventcodec/eventcodec_test.go) shows the effect: a message written with the old schema still decodes, with `vehicleType` set to the default.
- **Try to register an incompatible schema.** Schema Registry runs in `BACKWARD` mode: a new schema must be able to read data written with the old one. Adding a field *without* a default breaks that, so it's rejected (HTTP 409); with a default it's fine:
  ```bash
  # rejected: "vehicle" has no default
  curl -s -X POST localhost:8081/subjects/order.matched-value/versions -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
    -d '{"schema":"{\"type\":\"record\",\"name\":\"OrderMatched\",\"namespace\":\"grabmini.events\",\"fields\":[{\"name\":\"eventId\",\"type\":\"string\"},{\"name\":\"occurredAt\",\"type\":\"string\"},{\"name\":\"orderId\",\"type\":\"string\"},{\"name\":\"driverId\",\"type\":\"string\"},{\"name\":\"vehicle\",\"type\":\"string\"}]}"}'
  ```
- **Replay a topic and see nothing counted twice.** Ratings are averaged per driver from `trip.rated`, deduplicated by `eventId` in the same transaction as the update. Stop the Rating Service, rewind its consumer group, start it again, and the events are read again but the average doesn't change (`duplicate trip.rated event ..., skipping` in its log):
  ```bash
  pkill -f .dev/bin/rating-service   # then wait a few seconds for the group to go idle
  docker exec grab-mini-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
    --group rating-service-group --reset-offsets --to-earliest --topic trip.rated --execute
  cd services/rating-service && go run .
  ```
- **Replay an old location** (US-3.1). Publish a `driver.location.updated` with a `timestamp` in the past. The Driver Location Service logs `discarded as out-of-order` and the driver's position doesn't move.
- **Make every driver busy** and book a ride, to see `order.no_driver_found`:
  ```bash
  for n in 1 2 3 4 5; do docker exec grab-mini-redis redis-cli SET "driver:driver-${n}:status" ON_TRIP; done
  ```
  Put them back with `cd services/matching-service && go run . seed-drivers`. Quote the key or use `${n}`: in zsh, an unbraced `$n:status` is parsed as a modifier and writes the wrong key.

## Services

| Service | Port | Consumes | Produces | State |
|---|---|---|---|---|
| Order Service | 3001 | `order.matched`, `order.no_driver_found` (to keep order status current) | `order.created` | Postgres |
| Matching Service | | `order.created` | `order.matched`, `order.no_driver_found` | Redis (driver status + GEO) |
| Trip Service | 3002 | `order.matched` | `trip.status.changed`, `trip.rated` | Postgres, Redis (releases drivers) |
| Driver Location Service | | `driver.location.updated` | | Redis GEO |
| Rating Service | 3004 | `trip.rated` | | Postgres (per-driver totals) |
| ksqlDB | 8088 | every topic above | analytics tables (`analytics` section) | Kafka (changelog topics) |
| Notification Service | 3003 | all of the above | | WebSocket connections (per instance), Redis (routing state and Pub/Sub between instances) |
| Auth Service | 3005 | | | none (development token issuer) |
| Pricing Service | 3007 | `order.created` (demand per pickup cell) | | Redis (surge multipliers, demand counters); quotes are signed, not stored |
| Location Gateway | 3006 | | `driver.location.updated` | per-driver rate limit in memory |
| `driver-app-sim` | | | | reports positions to the Location Gateway over HTTP |

Order Service and Trip Service never publish straight from a request: the event is saved in an `outbox` table in the same Postgres transaction as the order, trip status or rating, and a relay ([services/pkg/outbox](../services/pkg/outbox)) publishes it to Kafka afterwards, so a crash can't leave a state change nobody was told about. Matching Service records which driver it gave each order (`match:<orderId>` in Redis), so a redelivered `order.created` can't claim a second driver, and a claimed driver is only *leased* (`ASSIGNED` for `ASSIGN_LEASE_SECONDS`, default 120) until Trip Service confirms the trip and sets them `ON_TRIP`. Trip Service also frees the driver from the `trip.status.changed` event (a safety net for a crash between saving the trip and the Redis update). Every service stops cleanly on SIGTERM / Ctrl-C.

A message a consumer can't handle right now (a database that is down) is not retried in place: it moves to `<topic>.retry.1`, then `.2`, `.3` (waiting 5s, 1m, 10m), so the partition keeps flowing; after the last tier, or at once for a message that can never succeed, it lands on `<topic>.DLQ`. Notification Service can run as several instances: events are consumed by any of them, and each message is published to a Redis channel for its recipient so the instance holding that socket delivers it.

### Security

Every API call and every WebSocket carries a JWT, and the services trust the token, not the request: a rider's id comes from their token (a body naming someone else is a 403), a rider sees only their own orders and trips, only a trip's driver can pick up or complete it, and only its rider can rate it. In development [Auth Service](../services/auth-service) signs a token for whoever asks (`curl -X POST localhost:3005/token -d '{"role":"rider","id":"rider-1"}'`), which is why it refuses to start with `APP_ENV=production`; the apps sign in through `@grab-mini/client`. A WebSocket sends its token as a subprotocol (not in the URL) and is only accepted from an origin in `ALLOWED_ORIGINS`.

Drivers' phones never talk to Kafka. [Location Gateway](../services/location-gateway) authenticates the driver, validates the position, limits each driver to one report a second, skips a parked driver's reports, and publishes in batches.

Credentials come from `NAME_FILE` (a mounted secret) or `NAME`, and with `APP_ENV=production` a missing one is an error rather than a development default: `DATABASE_URL`, `JWT_SECRET`, `KAFKA_PASSWORD`. For Kafka, set `KAFKA_SECURITY_PROTOCOL=SASL_SSL`, `KAFKA_USERNAME`, `KAFKA_PASSWORD_FILE` and `KAFKA_CA_FILE` (see [pkg/kafkaconn](../services/pkg/kafkaconn)); production refuses a plaintext connection. Each service has its own principal and `scripts/kafka-acls.sh` grants it only what it does (the table in that script is the source of truth). To try a Kafka with TLS, SASL/SCRAM and ACLs without Docker:

```bash
KAFKA_HOME=/path/to/kafka_2.13-3.8.0 deploy/kafka-secure/start-local.sh   # prints the environment to give a service
```

`docker compose` still starts an unauthenticated PLAINTEXT Kafka for development.

Each service owns its schema as versioned SQL in `services/<service>/migrations` (goose, a version table per service). They are applied at startup; `<service> migrate` applies them and exits, and `RUN_MIGRATIONS=false` skips them at startup. Every service with an HTTP API serves `/livez` (alive) and `/readyz` (its Postgres, Redis and Kafka answer; 503 once shutdown begins); Matching and Driver Location have no API, so set `HEALTH_PORT` to get the same two probes.

Every consumer runs through [services/pkg/kafkaconsumer](../services/pkg/kafkaconsumer): retry, then dead-letter topic, and uniform logging of topic/partition/offset. Every producer and consumer encodes and decodes through [services/pkg/eventcodec](../services/pkg/eventcodec) (Avro plus the Schema Registry wire format); the schemas live in [services/pkg/events/schemas](../services/pkg/events/schemas). Duplicate events are harmless (dedupe by `eventId`, idempotent state transitions).

`driver-app-sim` publishes the drivers' locations because a browser can't produce to Kafka directly; the Driver App only displays its position.

To run a single piece by hand instead of `pnpm dev:all` (with `docker compose up -d` already running):

```bash
cd services/matching-service && go run . seed-drivers   # once, to seed drivers
cd services/order-service && go run .                   # likewise for each service
```

### HTTP API

```bash
# Order Service
curl -X POST localhost:3001/orders -H "Content-Type: application/json" \
  -d '{"riderId": "rider-1", "pickup": {"lat": 10.776, "lng": 106.700}, "dropoff": {"lat": 10.79, "lng": 106.71}, "vehicleType": "BIKE"}'   # vehicleType: CAR (default) or BIKE
curl localhost:3001/orders/<orderId>                  # includes status: PENDING, then MATCHED or NO_DRIVER
curl localhost:3001/riders/<riderId>/latest-order     # what a reloaded Rider App uses to find its ride

# Trip Service
curl localhost:3002/trips/by-order/<orderId>
curl localhost:3002/trips/<tripId>
curl localhost:3002/drivers/<driverId>/active-trip    # the driver's ASSIGNED or IN_PROGRESS trip, else 404
curl -X POST localhost:3002/trips/<tripId>/pickup     # ASSIGNED -> IN_PROGRESS
curl -X POST localhost:3002/trips/<tripId>/complete   # IN_PROGRESS -> COMPLETED, driver becomes AVAILABLE
curl -X POST localhost:3002/trips/<tripId>/cancel     # from ASSIGNED or IN_PROGRESS; 409 once COMPLETED
curl -X POST localhost:3002/trips/<tripId>/rating -H "Content-Type: application/json" \
  -d '{"riderId": "rider-1", "stars": 5, "comment": "Smooth ride", "tipAmount": 20000}'   # COMPLETED trips only, once (409 otherwise); tipAmount is optional whole VND, 0 to 1,000,000

# Rating Service
curl localhost:3004/drivers/driver-1/rating          # {"average": 4.67, "count": 3, "totalTips": 30000, "tipCount": 2}
curl "localhost:3004/drivers/driver-1/reviews?limit=20&before=<createdAt>"   # a driver's own reviews, newest first (driver token only)
```

Repeating `pickup` or `complete` is a no-op. The WebSocket is `ws://localhost:3003/ws/rider/<id>` or `/ws/driver/<id>`, pushing `{"type": "...", "payload": {...}}`.

Inspect state directly:

```bash
docker exec grab-mini-redis redis-cli GEOPOS drivers:geo driver-1
docker exec grab-mini-redis redis-cli GET "driver:driver-1:status"
docker exec grab-mini-redis redis-cli GET "driver:driver-1:vehicle"
docker exec grab-mini-postgres psql -U grabmini -d grabmini -c "select * from trips;"
```

## Repository layout

**Analytics**: [ksql/statements.sql](../ksql/statements.sql), the ksqlDB streams and tables, applied by the `ksql-init` container.

**Backend**, a Go workspace ([go.work](../go.work)) with one module per service:
- `services/*-service` (order, matching, trip, driver-location, notification, rating), `services/driver-app-sim`
- `services/pkg/events`: Kafka event types, topic names and the Avro schemas (`schemas/*.avsc`)
- `services/pkg/eventcodec`: Avro encode/decode with Schema Registry (registers schemas, resolves writer vs reader schema)
- `services/pkg/kafkaconsumer`: the retry + dead-letter consumer loop
- `services/pkg/driverstatus`: the Redis driver status and vehicle keys, and the atomic claim (AVAILABLE and the right vehicle, or nothing)
- `services/pkg/driverlocation`: the Redis GEO key and the out-of-order-safe location update

**Frontend**, a pnpm workspace (React, TypeScript, Vite):
- `apps/rider`, `apps/driver`: the two apps, and `apps/ops`: the analytics dashboard (reads ksqlDB)
- `packages/ui`: the design system, with tokens (CSS variables, light and dark) and components
- `packages/client`: REST calls and the notification WebSocket hook
- `packages/events`: types for the WebSocket messages

Explore the design system on its own with `pnpm dev:ui` (http://localhost:5170). `pnpm --filter @grab-mini/ui build` produces the library build (ES module, `style.css`, type declarations); inside the workspace the apps import it from source.

In dev, the apps reach the Go services through Vite's proxy, since the services send no CORS headers.

## Automation Tools & Makefile

The project includes a comprehensive [Makefile](../Makefile) for development, infrastructure, testing, and operations. Run `make help` (or `make`) to see all targets:

```bash
make help
```

### Development & Build
- `make dev`: Starts the complete environment (Docker infra, Observability stack, Go services, driver simulator, web apps).
- `make dev-reset`: Wipes Docker volumes and resets everything from scratch (`scripts/dev.sh --reset`).
- `make build`: Compiles all Go binaries to `.dev/bin` and builds all frontend applications.
- `make seed`: Seeds the 5 default drivers (cars and bikes) into Redis.

### Infrastructure & Docker
- `make infra-up`: Starts all Docker containers (Kafka, Postgres, Redis, Schema Registry, ksqlDB, Prometheus, Loki, Promtail, Grafana).
- `make infra-down`: Stops all Docker containers (retaining volumes).
- `make infra-reset`: Stops all Docker containers and deletes volumes.
- `make infra-ps`: Lists status and health of all Docker containers.
- `make obs-up`: Starts only the Observability stack (`prometheus`, `loki`, `promtail`, `grafana`).

### Logging & Metrics CLI
- `make logs`: Tails all service logs in `.dev/logs/*.log`.
- `make logs SVC=order-service`: Tails logs of a specific service.
- `make logs-errors`: Filters all `WARN` and `ERROR` lines across `.dev/logs/*.log`.
- `make logs-loki SVC=matching-service LIMIT=50`: Queries recent logs directly from Loki via HTTP API.
- `make metrics-check`: Probes `/readyz`, counts active `/metrics` series across all 9 Go services, and lists active Prometheus scrape targets.
- `make prom-check`: Validates Prometheus configuration (`prometheus.yml`), alert rules (`alerts.rules.yaml`), and executes rule unit tests (`alerts.test.yaml`).

### Quality, Testing & Operations
- `make test`: Runs unit tests across all Go modules in `go.work`.
- `make test-integration`: Runs Go tests requiring real Postgres, Redis, and Kafka.
- `make fmt`: Formats all Go code (`gofmt -s -w`).
- `make lint`: Runs format check, `go vet`, and TypeScript `typecheck`.
- `make verify`: Executes [deploy/verify.sh](../deploy/verify.sh) (K8s manifests, CRDs, alert rules, static builds).
- `make migrate`: Runs SQL schema migrations for `order-service`, `trip-service`, and `rating-service`.
- `make kafka-topics`: Lists all Kafka topics in the cluster.
- `make kafka-lag`: Describes consumer group offsets and lag across all services.
- `make dlq-dry-run TOPIC=order.created`: Inspects dead-letter messages without redriving.
- `make dlq-redrive TOPIC=order.created`: Redrives dead-letter messages back to their source topic.
- `make loadtest RATE=50 DURATION=30s DRIVERS=100`: Runs the end-to-end load simulator.
- `make clean`: Removes `.dev/bin`, `.dev/logs`, and frontend `dist` build outputs.

## Observability (Metrics & Logging)

The local stack includes a full observability suite running via Docker Compose:

1. **Grafana (http://localhost:3000)**:
   - Log in automatically as Admin (anonymous authentication enabled).
   - Pre-configured dashboard: **`grab-mini — Observability (Metrics & Logs)`**, featuring:
     - **Service Health & HTTP Traffic**: Up/Down status, HTTP request rates by route & code, p50/p95 latency.
     - **Business Metrics**: Order creation rate, matching outcomes, matching latency vs 3s SLO, trip transitions, ratings, open WebSockets.
     - **Kafka Streaming & Outbox**: Consumer throughput (ok / retried / dead_lettered), outbox pending age, publish latency, surge multipliers.
     - **Centralized Logs (Loki)**: Live log stream and log volume over time.
   - Explore logs interactively in Grafana Explore with LogQL:
     - By service: `{job="grab-mini", service="order-service"}`
     - By severity: `{job="grab-mini", level="ERROR"}`
     - By distributed trace: `{job="grab-mini"} |= "<trace_id>"`
2. **Prometheus (http://localhost:9090)**:
   - Scrapes `/metrics` every 5s from all Go services (via `host.docker.internal`).
   - Evaluates alerting rules from [deploy/monitoring/alerts.rules.yaml](../deploy/monitoring/alerts.rules.yaml) (check status at http://localhost:9090/alerts).
3. **Loki (http://localhost:3100) & Promtail**:
   - Promtail tails `.dev/logs/*.log`, automatically extracts `service` from the filename, and parses structured `slog` JSON attributes (`level`, `service`, `trace_id`, `span_id`).

## Operating it

Every service logs JSON in production (with `trace_id` on lines inside a span) and serves Prometheus metrics; a request keeps one trace across HTTP, the outbox, Kafka and the retry topics. `deploy/` has a Dockerfile, a Kustomize base (Kubernetes, Strimzi Kafka across three zones, KEDA scaling on consumer lag, alerts) and `deploy/verify.sh` to check it without a cluster; [docs/runbook.md](runbook.md) says what to do when an alert fires, [docs/disaster-recovery.md](disaster-recovery.md) covers backups and failing over, and `deploy/loadtest` measures throughput without Docker ([results and what they mean](performance.md)). `dlq-redrive` sends dead letters back to their topic once the cause is fixed:

```bash
KAFKA_BROKERS=localhost:9092 go run ./services/dlq-redrive -topic order.created -dry-run
# or simply:
make dlq-dry-run TOPIC=order.created
```

## Running Tools Locally

### Design System Playground
```bash
pnpm dev:ui
```
Opens the UI components and design token explorer at http://localhost:5170.

### End-to-End Load Testing
```bash
# Run against the running `make dev` stack:
make loadtest RATE=30 DURATION=30s DRIVERS=50
```
Or start an isolated, non-Docker Go stack with `dev-registry` using [deploy/loadtest/stack.sh](../deploy/loadtest/stack.sh):
```bash
DATABASE_URL='postgres://grabmini:grabmini@localhost:5432/grabmini_load?sslmode=disable' deploy/loadtest/stack.sh start
go run ./services/loadtest -rate 50 -duration 30s -drivers 100
DATABASE_URL='postgres://grabmini:grabmini@localhost:5432/grabmini_load?sslmode=disable' deploy/loadtest/stack.sh stop
```

### Secure Kafka (TLS + SASL + ACLs)
```bash
KAFKA_HOME=/path/to/kafka_2.13-3.8.0 deploy/kafka-secure/start-local.sh
```

## Tests

```bash
# Run all Go unit tests + Frontend unit & component tests (Vitest)
make test

# Frontend unit & component tests only (Vitest + Testing Library)
pnpm test

# Browser E2E tests across Rider, Driver & Ops apps (Playwright)
pnpm test:e2e

# Go integration + full-stack system tests against local Postgres, Redis, Kafka
make test-integration

# Code quality and typecheck
make lint
```

Most Go tests need nothing. A few (including the full-stack system test in [services/e2e-test](../services/e2e-test)) run against a real Postgres, Redis, and Kafka and skip themselves unless you point them at a throwaway instance: `TEST_DATABASE_URL=postgres://grabmini:grabmini@localhost:5432/grabmini_test` and `TEST_REDIS_ADDR=localhost:6379`. Tests of the Kafka consumer, the outbox relay, the Location Gateway's producer, and `services/e2e-test` also want a broker: `TEST_KAFKA_BROKERS=localhost:9092`. The security tests in `services/pkg/kafkaconsumer` run against a broker made by `deploy/kafka-secure/start-local.sh`.

## Troubleshooting

- **`the Docker daemon isn't running`**: start Docker Desktop.
- **A port is already in use** (3000-3007, 5170-5173, 5432, 6379, 8080, 8081, 8088, 9090, 9092, 9101, 9102): stop whatever holds it. Vite silently moves to the next port if its own is taken, which breaks the URLs above; `lsof -nP -iTCP:<port> -sTCP:LISTEN` shows the culprit.
- **The rider stays on "Looking for a driver"**: check `.dev/logs/matching-service.log` and that the drivers are seeded (`docker exec grab-mini-redis redis-cli GEOPOS drivers:geo driver-1`).
- **The rider app is on a screen that doesn't match the trip**: reload it. It asks Order Service for the rider's latest order and Trip Service for its trip, and resumes searching, assigned or in-progress rides (a missed WebSocket push while searching is also caught by polling every 3s).
- **The driver app shows no trip**: check the `?driver=` id matches the driver the rider was matched with (the rider's card shows it). A trip already under way is restored when the page opens or reloads.
- **The Ops dashboard says ksqlDB is unreachable, or shows no data**: ksqlDB is a JVM and takes a minute to start; `docker logs grab-mini-ksqldb` shows its progress, and `docker logs grab-mini-ksql-init` shows whether the tables were created. The tables fill as events flow, so book a ride first.
- **Start from scratch**: `make dev-reset` (or `scripts/dev.sh --reset`).

