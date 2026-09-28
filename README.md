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
