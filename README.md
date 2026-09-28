# grab-mini

A mini ride-booking app built to learn Kafka (event streaming) and design systems.

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

pnpm workspace with:
- `apps/*` — rider and driver frontends (added in a later phase)
- `services/*` — backend services (order, matching, trip, driver-location, notification)
- `packages/events` — shared Kafka event types and topic names
- `packages/ui` — shared design system components (added in a later phase)

```bash
pnpm install
```

## Running the services (Phase 1)

With infra up (`docker compose up -d`) and dependencies installed:

```bash
# Order Service — HTTP API on :3001, publishes order.created
pnpm --filter @grab-mini/order-service start

# Matching Service — consumes order.created, publishes order.matched / order.no_driver_found
pnpm --filter @grab-mini/matching-service seed:drivers   # seed a few AVAILABLE drivers first
pnpm --filter @grab-mini/matching-service start
```

Create a ride order:

```bash
curl -X POST http://localhost:3001/orders \
  -H "Content-Type: application/json" \
  -d '{"riderId": "rider-1", "pickup": {"lat": 10.776, "lng": 106.700}, "dropoff": {"lat": 10.79, "lng": 106.71}}'
```

Watch the match happen in the Matching Service logs, or inspect the `order.created` / `order.matched` / `order.no_driver_found` topics in Kafka UI. Run a second Matching Service instance (`INSTANCE_ID=matching-B pnpm --filter @grab-mini/matching-service start`) to see the consumer group split partitions across instances with no duplicate processing.
