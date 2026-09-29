# grab-mini

A mini ride-booking app (think Grab) built to learn three things by building them: **Kafka** (event streaming), **Go**, and a **design system**.

A rider books a ride, a matching service finds the nearest driver, a trip runs through its lifecycle, and both apps update in real time. Every step between services is a Kafka event.

```
Rider App ──POST──> Order Service ──> [order.created] ──> Matching Service ──> [order.matched] ──┬──> Trip Service ──> [trip.status.changed] ──┐
                                                                     └──────> [order.no_driver_found]  │                                    │
Driver App (simulated) ──> [driver.location.updated] ──> Driver Location Service ──> Redis GEO        └────────────────────────────────────┴──> Notification Service ──WebSocket──> Rider / Driver App
```

The requirements, design and task breakdown live in [specs/ride-booking-mini](specs/ride-booking-mini).

## Quick start

**Prerequisites:** Docker Desktop (running), Go 1.26+, Node 20+, and pnpm 9 (`corepack enable`, or `npm i -g pnpm`).

```bash
pnpm install
pnpm dev:all
```

`pnpm dev:all` (which runs [scripts/dev.sh](scripts/dev.sh)) starts the Docker infra, waits for it to be healthy, seeds five drivers, builds and runs every Go service plus the driver simulator, and starts both apps. When it prints `grab-mini is up`:

| What | URL |
|---|---|
| Rider App | http://localhost:5171 |
| Driver App | http://localhost:5172/?driver=driver-1 |
| Kafka UI | http://localhost:8080 |

Ctrl-C stops the services and apps (the Docker infra keeps running). Logs are in `.dev/logs/<name>.log`. Use `scripts/dev.sh --reset` to wipe Kafka, Postgres and Redis data first, and `docker compose down` to stop the infra.

### Take a ride

1. In the **Rider App**, tap the map to drop a destination pin, then **Book ride**.
2. The rider is matched with the nearest driver (`driver-1` to `driver-5`, all seeded around central Ho Chi Minh City) and can watch them move on the map.
3. Open the **Driver App as that driver**, e.g. `http://localhost:5172/?driver=driver-2`. A driver only receives a trip while their app is open. **Confirm pickup**, then **Complete trip**, and the rider's timeline follows along.

Also try: **Cancel trip** from either side, or booking when every driver is busy (you get "No drivers nearby" after the matching service's retries). `?rider=rider-2` picks a different rider.

The map background comes from OpenStreetMap tiles, so it needs internet access; markers work without it.

## Watching Kafka work

Open [Kafka UI](http://localhost:8080), cluster **grab-mini**, while you take a ride:

- **Topics**: `order.created`, `order.matched`, `order.no_driver_found`, `trip.status.changed` and `driver.location.updated`, each with 3 partitions. Open a topic's **Messages** tab to see the JSON events. `driver.location.updated` grows every few seconds.
- **Partition keys**: messages for one order (or one driver) always land in the same partition, which keeps their order. Compare the **Partition** column across messages.
- **Consumers**: one consumer group per service (`matching-service-group`, `trip-service-group`, `driver-location-service-group`, `notification-service-group`). Each reads every event on its topics independently. Check **lag** here: it stays at or near 0 (a message or two in flight on `driver.location.updated` while the simulator publishes; `N/A` just means a group hasn't committed an offset on that topic yet).

## Things to try

- **Scale a consumer group.** Start a second matching service in another terminal and watch Kafka split the 3 partitions between the two instances, with no order matched twice:
  ```bash
  cd services/matching-service && INSTANCE_ID=matching-B go run .
  ```
  The **Consumers** tab in Kafka UI shows the partition assignment change.
- **Send a poison message and watch it land in a dead-letter topic.** Publish an event the Driver Location Service can't process; it retries 3 times (2s apart), then gives up and publishes to `driver.location.updated.DLQ` with the error and original payload attached:
  ```bash
  docker exec grab-mini-kafka bash -c 'echo "{\"eventId\":\"$(uuidgen)\",\"occurredAt\":\"2026-01-01T00:00:00Z\",\"driverId\":\"driver-99\",\"lat\":1.0,\"lng\":1.0,\"timestamp\":\"NOT-A-VALID-TIMESTAMP\"}" | /opt/kafka/bin/kafka-console-producer.sh --topic driver.location.updated --bootstrap-server localhost:9092'
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
| Order Service | 3001 | | `order.created` | Postgres |
| Matching Service | | `order.created` | `order.matched`, `order.no_driver_found` | Redis (driver status + GEO) |
| Trip Service | 3002 | `order.matched` | `trip.status.changed` | Postgres, Redis (releases drivers) |
| Driver Location Service | | `driver.location.updated` | | Redis GEO |
| Notification Service | 3003 | all of the above | | in-memory WebSocket connections |
| `driver-app-sim` | | | `driver.location.updated` | |

Every consumer runs through [services/pkg/kafkaconsumer](services/pkg/kafkaconsumer): retry, then dead-letter topic, and uniform logging of topic/partition/offset. Duplicate events are harmless (dedupe by `eventId`, idempotent state transitions).

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
  -d '{"riderId": "rider-1", "pickup": {"lat": 10.776, "lng": 106.700}, "dropoff": {"lat": 10.79, "lng": 106.71}}'
curl localhost:3001/orders/<orderId>

# Trip Service
curl localhost:3002/trips/by-order/<orderId>
curl localhost:3002/trips/<tripId>
curl -X POST localhost:3002/trips/<tripId>/pickup     # ASSIGNED -> IN_PROGRESS
curl -X POST localhost:3002/trips/<tripId>/complete   # IN_PROGRESS -> COMPLETED, driver becomes AVAILABLE
curl -X POST localhost:3002/trips/<tripId>/cancel     # from ASSIGNED or IN_PROGRESS; 409 once COMPLETED
```

Repeating `pickup` or `complete` is a no-op. The WebSocket is `ws://localhost:3003/ws/rider/<id>` or `/ws/driver/<id>`, pushing `{"type": "...", "payload": {...}}`.

Inspect state directly:

```bash
docker exec grab-mini-redis redis-cli GEOPOS drivers:geo driver-1
docker exec grab-mini-redis redis-cli GET "driver:driver-1:status"
docker exec grab-mini-postgres psql -U grabmini -d grabmini -c "select * from trips;"
```

## Repository layout

**Backend**, a Go workspace ([go.work](go.work)) with one module per service:
- `services/*-service`, `services/driver-app-sim`
- `services/pkg/events`: Kafka event types and topic names
- `services/pkg/kafkaconsumer`: the retry + dead-letter consumer loop
- `services/pkg/driverstatus`: the Redis driver-status key and its atomic claim
- `services/pkg/driverlocation`: the Redis GEO key and the out-of-order-safe location update

**Frontend**, a pnpm workspace (React, TypeScript, Vite):
- `apps/rider`, `apps/driver`: the two apps
- `packages/ui`: the design system, with tokens (CSS variables, light and dark) and components
- `packages/client`: REST calls and the notification WebSocket hook
- `packages/events`: types for the WebSocket messages

Explore the design system on its own with `pnpm dev:ui` (http://localhost:5170). `pnpm --filter @grab-mini/ui build` produces the library build (ES module, `style.css`, type declarations); inside the workspace the apps import it from source.

In dev, the apps reach the Go services through Vite's proxy, since the services send no CORS headers.

## Troubleshooting

- **`the Docker daemon isn't running`**: start Docker Desktop.
- **A port is already in use** (3001-3003, 5171-5172, 5432, 6379, 8080, 9092): stop whatever holds it. Vite silently moves to the next port if its own is taken, which breaks the URLs above; `lsof -nP -iTCP:5171 -sTCP:LISTEN` shows the culprit.
- **The rider stays on "Looking for a driver"**: check `.dev/logs/matching-service.log` and that the drivers are seeded (`docker exec grab-mini-redis redis-cli GEOPOS drivers:geo driver-1`).
- **The driver app shows no trip**: it must be open, as the matched driver, when the trip is assigned. Reload doesn't recover an in-flight trip.
- **Start from scratch**: `scripts/dev.sh --reset`.
