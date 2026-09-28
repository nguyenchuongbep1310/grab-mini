# Tasks — Ride-booking Mini

Reference: [requirements.md](./requirements.md), [design.md](./design.md)

Convention: each task should be a small commit/PR, with at least minimal tests before moving to the next task.

## Phase 0 — Foundation
- [x] 0.1 Initialize monorepo: Go workspace (`go.work`) for `services/*`, pnpm workspace for `apps/*` and `packages/ui`
- [x] 0.2 Docker Compose: Kafka (KRaft mode, no Zookeeper) + Kafka UI + Postgres + Redis
- [x] 0.3 Verify: manually create a topic, produce/consume via CLI to confirm the cluster works
- [x] 0.4 Create shared package `services/pkg/events` (Go) with type definitions for every Kafka event (per requirements.md). `packages/events` (TypeScript) is kept for later frontend WebSocket payload types.

> Note: the backend was switched from Node.js/TypeScript to **Go** after Phase 1 was first built, to also use this project to learn Go. Order Service and Matching Service were rewritten in Go; see [design.md](./design.md#7-tech-stack).

## Phase 1 — Order + Matching (first core Kafka loop)
- [x] 1.1 Order Service: create-order API, persist to Postgres, publish `order.created` (US-1.1)
- [x] 1.2 Matching Service: dedicated consumer group, consumes `order.created`
- [x] 1.3 Mock driver status: seed a few `AVAILABLE` drivers in Redis with geo data
- [x] 1.4 Matching logic: find nearest driver, publish `order.matched` or `order.no_driver_found` (US-2.1)
- [x] 1.5 Test: run 2 Matching Service instances in the same consumer group, verify no duplicate order processing (observe via Kafka UI)

## Phase 2 — Trip lifecycle
- [x] 2.1 Trip Service: consumes `order.matched`, creates trip as `ASSIGNED`
- [x] 2.2 Confirm-pickup API → `IN_PROGRESS`, publishes `trip.status.changed` (US-4.1)
- [x] 2.3 Complete-trip API → `COMPLETED`, releases driver (US-4.2)
- [x] 2.4 Cancel-trip API + validate valid state transitions (US-4.3)
- [x] 2.5 Idempotency: add `eventId` + dedupe check in Trip Service (US-6.1)

## Phase 3 — Driver location (learning a high-throughput topic)
- [x] 3.1 Driver Location Service: dedicated consumer group, writes to Redis GEO
- [x] 3.2 Handle out-of-order events: discard events with an older timestamp than the current record (US-3.1)
- [x] 3.3 Driver App: simulate sending location every 3-5s (mock GPS or manual input on a map) — implemented as a `driver-app-sim` CLI (real Driver App frontend comes in Phase 6)

## Phase 4 — Realtime notifications
- [x] 4.1 Notification Service: WebSocket server, map connections by riderId/driverId
- [x] 4.2 Consume `order.matched`, `order.no_driver_found`, `trip.status.changed` → push to the right client (US-5.1)
- [x] 4.3 Consume `driver.location.updated` → push driver location to the rider watching the corresponding trip

## Phase 5 — Resilience
- [ ] 5.1 Add retry + dead-letter topic for each consumer (US-6.2)
- [ ] 5.2 Simulate a failure: throw in a consumer, verify the event lands in the DLQ after N retries
- [ ] 5.3 Standardize logging: each consumer logs topic/partition/offset on processing (for observability)

## Phase 6 — Design system
- [ ] 6.1 `packages/ui`: set up build (Vite lib mode) + tokens (color, spacing, typography)
- [ ] 6.2 Components: `Button`, `Card`, `Avatar`, `StatusBadge`, `Toast`
- [ ] 6.3 Components: `BottomSheet`, `TripStatusTimeline`
- [ ] 6.4 Components: `MapView` + `MapMarker` (wraps MapLibre/Leaflet)
- [ ] 6.5 Apply all components in the Rider App (home, booking, tracking)
- [ ] 6.6 Apply all components in the Driver App (accept trip, tracking, completion)

## Phase 7 — End-to-end integration
- [ ] 7.1 Run the full flow: rider books → matched → driver moves → completed, observe end to end via Kafka UI
- [ ] 7.2 Write a README describing how to run locally (docker compose up, seed data, run each app)

## Backlog (after core is done, explore as interested)
- [ ] Post-trip rating/feedback
- [ ] Multiple vehicle types (car/bike) affecting matching
- [ ] Kafka Streams/ksqlDB for analytics instead of manual consumers
- [ ] Schema Registry (Avro/Protobuf) instead of plain JSON — learn schema evolution
