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
| Order Service | Accepts booking requests, manages orders | — | `order.created` | Postgres |
| Matching Service | Finds the nearest driver | `order.created` | `order.matched`, `order.no_driver_found` | Redis (driver status/geo) |
| Driver Location Service | Receives & stores driver location | `driver.location.updated` | — | Redis (geo, TTL) |
| Trip Service | Trip state machine | `order.matched` | `trip.status.changed` | Postgres |
| Notification Service | Pushes realtime updates to clients | `order.matched`, `order.no_driver_found`, `trip.status.changed`, `driver.location.updated` | — | (stateless, in-memory socket map) |
| Analytics Service (optional) | Logs all events to practice a separate consumer group | all topics | — | append-only log file / Postgres |

## 3. Kafka topics

| Topic | Partition key | Partitions (local) | Notes |
|---|---|---|---|
| `order.created` | `orderId` | 3 | |
| `order.matched` | `orderId` | 3 | |
| `order.no_driver_found` | `orderId` | 3 | |
| `driver.location.updated` | `driverId` | 3 | Highest throughput; keyed by driver to preserve per-driver ordering |
| `trip.status.changed` | `tripId` | 3 | |
| `*.DLQ` | same as source topic | 1 | Dead-letter topic per topic above |

Consumer group naming: `<service-name>-group` (e.g. `matching-service-group`) so each service scales independently without reading each other's events.

## 4. Data model (simplified)

**Order** (Postgres)
```
id, riderId, pickupLat, pickupLng, dropoffLat, dropoffLng, vehicleType[CAR|BIKE], status[PENDING|MATCHED|NO_DRIVER], createdAt
```

**Trip** (Postgres)
```
id, orderId, driverId, status[ASSIGNED|IN_PROGRESS|COMPLETED|CANCELLED], startedAt, completedAt
```

**Driver status** (Redis)
```
driver:{id}:status -> AVAILABLE | ASSIGNED | ON_TRIP
driver:{id}:vehicle -> CAR | BIKE
driver:{id}:geo -> lat, lng (Redis GEO)
```

## 4b. Event serialization

Every event is Avro in Confluent's Schema Registry wire format (magic byte, 4-byte schema id, Avro payload). Schemas live in `services/pkg/events/schemas/*.avsc`, one per topic, registered under the subject `<topic>-value`. The registry enforces `BACKWARD` compatibility, so a new schema version must be able to read data written with the old one; in practice, new fields need a default. Consumers read the writer's schema id from each message and resolve it against their own schema, so producers and consumers can be upgraded independently. Vehicle types are the worked example: `vehicleType` was added to `order.created` and `order.matched` as schema version 2 with a default of `CAR`, which is backward compatible. Matching claims a driver only if their status is AVAILABLE *and* their vehicle matches, in one atomic Redis script. A message that can't be decoded is a permanent error and goes straight to the dead-letter topic.

## 5. Idempotency strategy
- Every event carries an `eventId` (UUID) generated at the producer.
- Before applying a side effect, the consumer checks `processed_event_id` (a Redis set with TTL, or a Postgres table); if it already exists, skip.
- The trip state machine is naturally idempotent by target state (e.g. receiving `trip.status.changed → COMPLETED` again when already `COMPLETED` is a no-op).

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
