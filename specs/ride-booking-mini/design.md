# Design — Ride-booking Mini

Reference: [requirements.md](./requirements.md)

## 1. Architecture overview

```
Rider App ──HTTP──> Order Service ──publish──> [order.created]
                                                      │
                                                      ▼
                                          Matching Service (consumer group: matching)
                                                      │ publish
                                                      ▼
                                          [order.matched] / [order.no_driver_found]
                                                      │
                     ┌────────────────────────────────┼────────────────────────────┐
                     ▼                                ▼                            ▼
             Trip Service                  Notification Service           Analytics Service
        (consumer group: trip)          (consumer group: notify)      (consumer group: analytics)
                     │                                │
                     ▼                                ▼
          [trip.status.changed]                WebSocket → Rider/Driver App

Driver App ──publish──> [driver.location.updated] ──> Driver Location Service ──> Redis (geo)
                                                                 │
                                                                 └──> Notification Service (reads to push location)
```

Principle: **clients never connect to Kafka directly**. All realtime updates to apps go through the Notification Service (WebSocket).

## 2. Services

| Service | Responsibility | Consumes | Produces | Storage |
|---|---|---|---|---|
| Order Service | Accepts booking requests, manages orders, keeps each order's status current | `order.matched`, `order.no_driver_found` | `order.created` | Postgres |
| Pricing Service | Surge pricing: counts demand per pickup cell, samples supply, keeps a multiplier per cell, issues signed price quotes (`GET /quotes`) | `order.created` | | Redis (demand counters, multipliers); no Postgres |
| Matching Service | Finds the nearest driver | `order.created` | `order.matched`, `order.no_driver_found` | Redis (driver status/geo) |
| Driver Location Service | Receives & stores driver location | `driver.location.updated` | — | Redis (geo, TTL) |
| Trip Service | Trip state machine | `order.matched` | `trip.status.changed` | Postgres |
| Notification Service | Pushes realtime updates to clients | `order.matched`, `order.no_driver_found`, `trip.status.changed`, `driver.location.updated` | — | (stateless, in-memory socket map) |
| Rating Service | Averages each driver's ratings | `trip.rated` | | Postgres (`driver_ratings` with tip totals, `driver_reviews`, deduped by eventId in the same transaction) |
| ksqlDB (replaces the planned Analytics Service) | Stream processing: per-minute counts, a stream-stream join for match latency, ratings, throughput | all topics | analytics tables | Kafka (changelog topics) |

## 3. Kafka topics

| Topic | Partition key | Partitions (local) | Notes |
|---|---|---|---|
| `order.created` | `orderId` | 3 | |
| `order.matched` | `orderId` | 3 | |
| `order.no_driver_found` | `orderId` | 3 | |
| `order.cancelled` | `orderId` | 3 | Published by Order Service (via the outbox) when a rider cancels a `PENDING` order. Fields: `riderId`, `cancelledBy`, `reason` (all defaulted, so the schema stays backward compatible). Consumed by Matching (Redis key `order:{id}:cancelled`, 24h), Trip Service (`trip-order-cancelled-group`) and Notification |
| `driver.location.updated` | `driverId` | 3 | Highest throughput; keyed by driver to preserve per-driver ordering |
| `trip.status.changed` | `tripId` | 3 | On `CANCELLED` also carries `cancelledBy` (`RIDER`/`DRIVER`) and `reason`; Order Service consumes it to move a `MATCHED` order to `CANCELLED` |
| `trip.rated` | `driverId` | 3 | Published by Trip Service when a rider rates a COMPLETED trip (once per trip); carries `comment`, `tipAmount` (whole VND, 0 to 1,000,000, recorded only) and `currency` (`VND`), both added as defaulted Avro fields |
| `*.DLQ` | same as source topic | 1 | Dead-letter topic per topic above |

Consumer group naming: `<service-name>-group` (e.g. `matching-service-group`) so each service scales independently without reading each other's events.

## 4. Data model (simplified)

**Order** (Postgres)
```
id, riderId, pickupLat, pickupLng, dropoffLat, dropoffLng, vehicleType[CAR|BIKE], status[PENDING|MATCHED|NO_DRIVER|CANCELLED], createdAt, cancelledAt, cancelledBy, cancelReason
(status starts PENDING and is moved by Order Service's own consumer of the matching outcomes; `POST /orders/{id}/cancel` moves PENDING -> CANCELLED with a conditional UPDATE, so it and a concurrent order.matched cannot both win; trip cancellations move MATCHED -> CANCELLED through a separate function)
```

**Trip** (Postgres)
```
id, orderId, driverId, status[ASSIGNED|IN_PROGRESS|COMPLETED|CANCELLED], startedAt, completedAt, cancelledAt, cancelledBy, cancelReason, rating, ratingComment, tipAmount, ratedAt
(cancelled_orders: order ids whose order.cancelled was seen, so a late order.matched creates no trip; both consumers take a per-order advisory lock)
```

**Driver status** (Redis)
```
driver:{id}:status -> AVAILABLE | ASSIGNED | ON_TRIP
driver:{id}:vehicle -> CAR | BIKE
driver:{id}:geo -> lat, lng (Redis GEO)
```

## 4a. Surge pricing

- **Cells.** A pickup falls in the cell `floor(lat/deg):floor(lng/deg)`, the same grid as the driver GEO shards (`GEO_SHARD_DEGREES`; `SURGE_CELL_DEGREES` overrides; default 0.05 degrees, about 5.5 km). The pure functions live in `services/pkg/surge`.
- **Demand.** Pricing Service consumes `order.created` (group `pricing-service-group`) and does `INCR surge:demand:{vehicle}:{cell}:{minute}` (TTL 5 minutes). Active cells are remembered in the set `surge:cells`.
- **Supply and multiplier.** Every 5 seconds one instance (a short Redis lock) takes each active cell: supply = AVAILABLE drivers of that vehicle type within the equal-area radius of the cell centre (`driverlocation.Search` plus `driverstatus.AvailableVehicles`), demand = average orders per minute over the last 3 minute buckets, `target = clamp(1 + k * (demand / max(supply, 1) - 1), 1.0, MAX)` rounded to 0.1 (k = 0.5, MAX = 3.0), then smoothed with an EMA and a per-tick change cap (0.3) so prices do not jump. The result is `SET surge:mult:{vehicle}:{cell}` with a 30 second TTL; a missing key means 1.0.
- **Quote.** `GET /quotes?pickupLat&pickupLng&dropoffLat&dropoffLng&vehicleType` (Rider token) returns `{quoteId, baseFare, multiplier, price, currency: "VND", expiresAt}`. Base fare is a flag fall plus a per-km rate on the straight-line distance, with a minimum. `quoteId = base64url(body) "." base64url(HMAC-SHA256)`; the body holds the fare, multiplier, price, expiry and a fingerprint of rider, vehicle, pickup and dropoff, so nothing is stored. Pricing Service and Order Service share `QUOTE_SECRET` (`pkg/secret`: file or env, no dev default in production).
- **Booking.** `POST /orders` takes an optional `quoteId`: valid means the quoted price is locked; absent or expired means the multiplier is read from Redis and the price locked at booking; an expired quote whose multiplier moved by more than `SURGE_QUOTE_TOLERANCE` (0.2) gets `409 {newQuote}` for the rider to confirm. The order stores `surge_multiplier` and `price` (migration 00006) and `order.created` v3 carries `surgeMultiplier` (default 1.0) and `price` (nullable), so older messages and consumers still work.
- **Failing open.** Redis errors, timeouts or an absent key all price at 1.0 (counted in `pricing_fail_open_total` / `orders_surge_fail_open_total`, alert `SurgePricingFailOpen`). A stopped sampler lets multipliers expire after 30 seconds (alert `SurgeSamplerStalled`).
- **Frontend.** `getQuote()` in `packages/client`, the `useQuote` hook in the Rider App (debounced refetch, refresh before expiry, re-confirm after a 409), and `PriceTag` / `SurgeBadge` in the design system. Ops shows the `surge_demand` ksqlDB table.

## 4b. Event serialization

Every event is Avro in Confluent's Schema Registry wire format (magic byte, 4-byte schema id, Avro payload). Schemas live in `services/pkg/events/schemas/*.avsc`, one per topic, registered under the subject `<topic>-value`. The registry enforces `BACKWARD` compatibility, so a new schema version must be able to read data written with the old one; in practice, new fields need a default. Consumers read the writer's schema id from each message and resolve it against their own schema, so producers and consumers can be upgraded independently. Vehicle types are the worked example: `vehicleType` was added to `order.created` and `order.matched` as schema version 2 with a default of `CAR`, which is backward compatible. Matching claims a driver only if their status is AVAILABLE *and* their vehicle matches, in one atomic Redis script. A message that can't be decoded is a permanent error and goes straight to the dead-letter topic.

## 4c. Analytics

Rather than another hand-written consumer, analytics run in ksqlDB (see `ksql/statements.sql`): each Kafka topic is declared as a stream, and tables are derived with windowed aggregations and one stream-stream join (`order.created` with `order.matched` within a minute, to measure matching latency). Columns are inferred from the Avro schemas in the Schema Registry, which is why `schema-init` registers every schema before ksqlDB starts. The Ops dashboard reads the tables with pull queries. ksqlDB's own state is rebuilt from Kafka, so nothing here needs a database.

## 4d. Recovering after a reload

WebSocket pushes only reach a client that is connected at that moment, so both apps also recover by asking. The Driver App reads `GET /drivers/{id}/active-trip` (Trip Service). The Rider App reads `GET /riders/{id}/latest-order` (Order Service), and for a MATCHED order the trip from Trip Service; a still-PENDING order resumes "searching" and is polled every few seconds, which also covers a push missed while the socket reconnects. Composing the two calls in the client keeps the services independent (the trip doesn't know its rider, the order doesn't know its trip).

## 5. Idempotency strategy
- Every event carries an `eventId` (UUID) generated at the producer.
- Before applying a side effect, the consumer checks `processed_event_id` (a Redis set with TTL, or a Postgres table); if it already exists, skip.
- The trip state machine is naturally idempotent by target state (e.g. receiving `trip.status.changed → COMPLETED` again when already `COMPLETED` is a no-op).
- **Idempotency-Key on `POST /orders`** (migration `order-service/00009`). Optional header, 1 to 128 printable ASCII characters. `idempotency_keys(rider_id, key, order_id, fingerprint, status_code, response, created_at)`, primary key `(rider_id, key)`, is written in the same transaction as the order and its outbox row. The fingerprint is a SHA-256 of the normalized body (pickup, dropoff, vehicle type, quoteId; not the rider). Same key and body: the stored response is replayed (same `orderId`, same 201, header `Idempotent-Replayed: true`) and nothing is published. Same key, other body: 422. Concurrent duplicates serialize on a Postgres advisory lock on the key and the primary key is the backstop, so one order is created. A key is honoured for 24 hours; Order Service deletes older rows hourly (not part of the k8s retention CronJobs). The replay is answered before the quote is checked.
- **A quote buys one booking.** When Order Service honours a valid `quoteId` it marks it used with Redis `SET NX` (`quote:used:{sha256(quoteId)}`, TTL = the quote's remaining life plus 1s). A second booking with that quote gets `409 {error, newQuote}` like the re-quote path. Failure policy: if Redis cannot be reached the booking is not rejected but priced at the current multiplier (never the stale quote), counted in `orders_pricing_total{result="quote_unchecked"}`; a booking that fails after claiming gives the quote back.
- **Driver proximity (Trip Service).** `order.matched` (schema v3, optional `pickup`/`dropoff`) gives the trip its route (migration `trip-service/00009`). `POST /trips/{id}/pickup` needs the driver's last reported location (`pkg/driverlocation`, the data Matching searches) within `PICKUP_RADIUS_M` (200) of the pickup; `complete` within `DROPOFF_RADIUS_M` (300) of the dropoff. Refusals: 409 `driver_too_far`, 409 `driver_location_unknown`, 409 `driver_location_stale` (older than `LOCATION_MAX_AGE`, 60s); Redis unreachable is a 503 (fail closed). Repeating a step that already happened stays a no-op. A trip with no stored route (created before the migration) skips the check. `PROXIMITY_CHECK=off` disables it and is refused at startup unless `APP_ENV=development`; `make dev` and the loadtest stack set it because the simulator never drives to a rider.

## 6. Design system (frontend)

Shared between Rider App and Driver App (React + TypeScript), organized as its own package `packages/ui`:

**Tokens**: color, spacing, radius, typography — defined once, imported by both apps.

**Core components (phase 1)**:
- `Button`, `Card`, `BottomSheet`, `StatusBadge` (maps to order/trip status), `Avatar`, `Toast`
- `MapView` + `MapMarker` (wraps a map library, e.g. MapLibre/Leaflet)
- `TripStatusTimeline` (shows trip progress)

Principle: components hold no business logic or direct API calls — they receive data via props, leaving data-fetching to each app. Both apps share `packages/client` (REST calls + the notification WebSocket hook) and, in dev, reach the Go services through a Vite proxy (the services send no CORS headers).

## 7. Tech stack
- Backend: Go, standard `net/http` for HTTP-facing services, [segmentio/kafka-go](https://github.com/segmentio/kafka-go) for Kafka, [hamba/avro](https://github.com/hamba/avro) with Confluent Schema Registry for event schemas, [jackc/pgx](https://github.com/jackc/pgx) for Postgres, [redis/go-redis](https://github.com/redis/go-redis) for Redis. Chosen over Node.js/NestJS so the project also serves as a Go learning exercise, alongside Kafka and design systems.
- Kafka: Docker Compose (Kafka + Kafka UI), single local broker using KRaft (no Zookeeper needed)
- DB: Postgres (order, trip), Redis (driver status/geo, idempotency keys)
- Frontend: React + TypeScript + Vite, WebSocket (socket.io or plain ws)
- Monorepo layout: a Go workspace (`go.work`) linking `services/*` Go modules plus a shared `services/pkg/events` module for Kafka event types; a pnpm workspace for the frontend (`apps/rider`, `apps/driver`, `packages/ui`, and `packages/events` for WebSocket payload types shared by the two frontend apps)

## 8. Observability & debugging (supports the Kafka learning goal)
- Kafka UI to visually inspect topics, partitions, consumer lag
- Each service logs clearly: event received, partition/offset, processing time — to make consumer group behavior visible when scaling multiple instances
