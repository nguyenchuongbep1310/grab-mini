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

**Frontend** (added in a later phase) — a pnpm workspace:
- `apps/rider`, `apps/driver` — rider and driver frontends
- `packages/ui` — shared design system components
- `packages/events` — TypeScript types for WebSocket payloads sent to the frontend

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
