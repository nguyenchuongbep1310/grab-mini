# grab-mini

A mini ride-booking app built to learn Kafka (event streaming), Go, and design systems.

See [specs/ride-booking-mini](specs/ride-booking-mini) for requirements, architecture, and the task breakdown.

## Local infrastructure

Starts Kafka (KRaft mode, no Zookeeper), Kafka UI, Postgres, and Redis:

```bash
docker compose up -d
```

- Kafka broker: `localhost:9092`
- Kafka UI: [http://localhost:8080](http://localhost:8080)
- Postgres: `localhost:5432` (user/password/db: `grabmini`)
- Redis: `localhost:6379`

Stop everything:

```bash
docker compose down
```

Add `-v` to also wipe volumes (Kafka topics, Postgres data, Redis data).

## Workspace

**Backend** — a Go workspace (`go.work`):
- `services/order-service`, `services/matching-service`, `services/trip-service`, ... — one Go module per service
- `services/pkg/events` — shared Kafka event types and topic names
- `services/pkg/driverstatus` — shared Redis driver-status key format and atomic claim/release, used by any service that reads or moves a driver between AVAILABLE / ASSIGNED / ON_TRIP
- `services/pkg/driverlocation` — shared Redis GEO key and out-of-order-safe location update, used by Matching Service (reads) and Driver Location Service (writes)
- `services/pkg/kafkaconsumer` — the shared retry + dead-letter-topic consumer loop used by every consumer in the project
- `services/driver-app-sim` — publishes `driver.location.updated` every 3-5s for a set of drivers (a browser can't produce to Kafka, so the Driver App relies on this for its location)

**Frontend** — a pnpm workspace (React + TypeScript + Vite):
- `apps/rider`, `apps/driver` — rider and driver apps
- `packages/ui` — the design system: tokens (CSS variables, light/dark) and components
- `packages/client` — REST calls and the notification WebSocket hook shared by both apps
- `packages/events` — TypeScript types for the WebSocket payloads sent to the frontend

## Running the services (Phase 1)

With infra up (`docker compose up -d`):

```bash
# Order Service — HTTP API on :3001, publishes order.created
cd services/order-service && go run .

# Matching Service — consumes order.created, publishes order.matched / order.no_driver_found
cd services/matching-service && go run . seed-drivers   # seed a few AVAILABLE drivers first
cd services/matching-service && go run .
```

Create a ride order:

```bash
curl -X POST http://localhost:3001/orders \
  -H "Content-Type: application/json" \
  -d '{"riderId": "rider-1", "pickup": {"lat": 10.776, "lng": 106.700}, "dropoff": {"lat": 10.79, "lng": 106.71}}'
```

Watch the match happen in the Matching Service logs, or inspect the `order.created` / `order.matched` / `order.no_driver_found` topics in Kafka UI. Run a second Matching Service instance in another terminal (`cd services/matching-service && INSTANCE_ID=matching-B go run .`) to see the consumer group split partitions across instances with no duplicate processing.

## Running the services (Phase 2)

```bash
# Trip Service — HTTP API on :3002, consumes order.matched, publishes trip.status.changed
cd services/trip-service && go run .
```

Trip Service creates a trip (status `ASSIGNED`) automatically when it sees `order.matched`. Drive it through its lifecycle:

```bash
curl -X POST http://localhost:3002/trips/<tripId>/pickup    # -> IN_PROGRESS (US-4.1)
curl -X POST http://localhost:3002/trips/<tripId>/complete  # -> COMPLETED, releases the driver (US-4.2)
curl -X POST http://localhost:3002/trips/<tripId>/cancel    # -> CANCELLED, only from ASSIGNED/IN_PROGRESS (US-4.3)
curl http://localhost:3002/trips/<tripId>                   # inspect current status
```

Calling `pickup` or `complete` again after it already happened is a no-op (US-6.1); cancelling a `COMPLETED` trip returns `409`.

## Running the services (Phase 3)

```bash
# Driver Location Service — consumes driver.location.updated, writes to Redis GEO
cd services/driver-location-service && go run .

# Driver App simulator — publishes locations for driver-1..driver-5 (the seeded drivers) every 3-5s
cd services/driver-app-sim && go run .
# or a specific set: DRIVER_IDS=driver-1,driver-2 go run .
```

Check a driver's current position and last-update time:

```bash
docker exec grab-mini-redis redis-cli GEOPOS drivers:geo driver-1
docker exec grab-mini-redis redis-cli GET driver:driver-1:location:ts
```

An update with an older timestamp than what's recorded is discarded rather than applied (US-3.1) — the Driver Location Service logs `discarded as out-of-order` for it.

## Running the services (Phase 4)

```bash
# Notification Service — WebSocket server on :3003, pushes realtime updates
cd services/notification-service && go run .
```

Connect as a rider or driver (any WebSocket client works, e.g. `websocat`):

```bash
websocat ws://localhost:3003/ws/rider/rider-1
websocat ws://localhost:3003/ws/driver/driver-1
```

While connected as `rider-1`, create an order with `"riderId": "rider-1"` (see Phase 1) — the rider socket receives `order.matched` (or `order.no_driver_found`), then `trip.status.changed` as the trip progresses, and `driver.location.updated` for the matched driver once the driver app simulator (Phase 3) is running — all pushed as `{"type": "...", "payload": {...}}` JSON messages (US-5.1, task 4.3). The matched driver's own socket also receives `order.matched` and `trip.status.changed`.

## Resilience: retries and dead-letter topics (Phase 5)

Every consumer in this project runs through `services/pkg/kafkaconsumer`, which retries a failed message (3 attempts by default, 2s apart) and, if it still fails, publishes it — with the error and original payload attached — to `<topic>.DLQ` instead of blocking the partition or crashing (US-6.2). Every fetch also logs its topic/partition/offset (task 5.3).

To see it happen, publish a message the consumer can't process — e.g. an unparseable `timestamp` for Driver Location Service:

```bash
docker exec grab-mini-kafka bash -c 'echo "{\"eventId\":\"$(uuidgen)\",\"occurredAt\":\"2026-01-01T00:00:00Z\",\"driverId\":\"driver-99\",\"lat\":1.0,\"lng\":1.0,\"timestamp\":\"NOT-A-VALID-TIMESTAMP\"}" | /opt/kafka/bin/kafka-console-producer.sh --topic driver.location.updated --bootstrap-server localhost:9092'
```

Watch the service log 4 failed attempts (1 initial + 3 retries) and then `gave up after retries, published to driver.location.updated.DLQ`. Inspect it:

```bash
docker exec grab-mini-kafka /opt/kafka/bin/kafka-console-consumer.sh --topic driver.location.updated.DLQ --bootstrap-server localhost:9092 --from-beginning
```

## Running the apps and design system (Phase 6)

```bash
pnpm install
pnpm dev:ui       # design system playground   -> http://localhost:5170
pnpm dev:rider    # Rider App                   -> http://localhost:5171
pnpm dev:driver   # Driver App                  -> http://localhost:5172
```

The apps talk to the Go services through Vite's dev proxy (the services send no CORS headers), so all services from the earlier phases need to be running, plus `driver-app-sim` so drivers have a location.

End-to-end walkthrough:

1. Open the Rider App. Tap the map to drop a destination pin, then **Book ride**.
2. The rider sees the match (`driver-N`), then the driver moving on the map.
3. Open the Driver App as the matched driver: `http://localhost:5172/?driver=driver-N` (a driver only receives trips while their app is open). Use **Confirm pickup**, then **Complete trip**. The rider's timeline follows along.

`?rider=rider-2` / `?driver=driver-3` pick a different identity. Map tiles come from OpenStreetMap, so the map background needs internet access.

Build the design system as a library (ES module + `style.css` + type declarations in `packages/ui/dist`):

```bash
pnpm --filter @grab-mini/ui build
```

Inside this workspace the apps import `@grab-mini/ui` from source, so no build step is needed during development.
