# grab-mini

A mini ride-booking app (think Grab) built to learn three things by building them: **Kafka** (event streaming), **Go**, and a **design system**.

A rider books a ride, a matching service finds the nearest driver, a trip runs through its lifecycle, and both apps update in real time. Every step between services is a Kafka event, serialized as Avro and validated against a schema in Schema Registry.

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

`pnpm dev:all` (which runs [scripts/dev.sh](scripts/dev.sh)) starts the Docker infra (Kafka, Schema Registry, Postgres, Redis, Kafka UI), waits for it to be healthy, seeds five drivers, builds and runs every Go service plus the driver simulator, and starts both apps. When it prints `grab-mini is up`:

| What | URL |
|---|---|
| Rider App | http://localhost:5171 |
| Driver App | http://localhost:5172/?driver=driver-1 |
| Kafka UI | http://localhost:8080 |
| Schema Registry | http://localhost:8081/subjects |

Ctrl-C stops the services and apps (the Docker infra keeps running). Logs are in `.dev/logs/<name>.log`. Use `scripts/dev.sh --reset` to wipe Kafka, Postgres and Redis data first, and `docker compose down` to stop the infra.

### Take a ride

1. In the **Rider App**, pick **Car** or **Bike**, tap the map to drop a destination pin, then **Book ride**.
2. The rider is matched with the nearest driver of that type and can watch them move on the map. Five drivers are seeded around central Ho Chi Minh City: `driver-1` to `driver-3` drive cars, `driver-4` and `driver-5` drive bikes.
3. Open the **Driver App as that driver**, e.g. `http://localhost:5172/?driver=driver-2`. A driver only receives a trip while their app is open. **Confirm pickup**, then **Complete trip**, and the rider's timeline follows along.

Also try: booking a bike (only `driver-4`/`driver-5` can take it), **Cancel trip** from either side, or booking when every driver is busy (you get "No drivers nearby" after the matching service's retries). `?rider=rider-2` picks a different rider.

The map background comes from OpenStreetMap tiles, so it needs internet access; markers work without it.

## Watching Kafka work

Open [Kafka UI](http://localhost:8080), cluster **grab-mini**, while you take a ride:

- **Topics**: `order.created`, `order.matched`, `order.no_driver_found`, `trip.status.changed` and `driver.location.updated`, each with 3 partitions. Open a topic's **Messages** tab: Kafka UI decodes the Avro values through Schema Registry, so you see JSON. `driver.location.updated` grows every few seconds.
- **Partition keys**: messages for one order (or one driver) always land in the same partition, which keeps their order. Compare the **Partition** column across messages.
- **Schema Registry** (menu item in Kafka UI, or `curl localhost:8081/subjects`): one subject per topic (`order.created-value`, ...), each holding the Avro schema producers registered.
- **Consumers**: one consumer group per service (`matching-service-group`, `trip-service-group`, `driver-location-service-group`, `notification-service-group`). Each reads every event on its topics independently. Check **lag** here: it stays at or near 0 (a message or two in flight on `driver.location.updated` while the simulator publishes; `N/A` just means a group hasn't committed an offset on that topic yet).

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
- **See a schema evolve.** Vehicle types were added after the first release by giving `order.created` and `order.matched` a `vehicleType` field with a default of `CAR` (schema version 2). `curl localhost:8081/subjects/order.created-value/versions` lists both versions. Old messages still in the topic decode with the default, and a consumer on the old schema ignores the new field.
- **Try to register an incompatible schema.** Schema Registry runs in `BACKWARD` mode: a new schema must be able to read data written with the old one. Adding a field *without* a default breaks that, so it's rejected (HTTP 409); with a default it's fine:
  ```bash
  # rejected: "vehicle" has no default
  curl -s -X POST localhost:8081/subjects/order.matched-value/versions -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
    -d '{"schema":"{\"type\":\"record\",\"name\":\"OrderMatched\",\"namespace\":\"grabmini.events\",\"fields\":[{\"name\":\"eventId\",\"type\":\"string\"},{\"name\":\"occurredAt\",\"type\":\"string\"},{\"name\":\"orderId\",\"type\":\"string\"},{\"name\":\"driverId\",\"type\":\"string\"},{\"name\":\"vehicle\",\"type\":\"string\"}]}"}'
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

Every consumer runs through [services/pkg/kafkaconsumer](services/pkg/kafkaconsumer): retry, then dead-letter topic, and uniform logging of topic/partition/offset. Every producer and consumer encodes and decodes through [services/pkg/eventcodec](services/pkg/eventcodec) (Avro plus the Schema Registry wire format); the schemas live in [services/pkg/events/schemas](services/pkg/events/schemas). Duplicate events are harmless (dedupe by `eventId`, idempotent state transitions).

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
docker exec grab-mini-redis redis-cli GET "driver:driver-1:vehicle"
docker exec grab-mini-postgres psql -U grabmini -d grabmini -c "select * from trips;"
```

## Repository layout

**Backend**, a Go workspace ([go.work](go.work)) with one module per service:
- `services/*-service`, `services/driver-app-sim`
- `services/pkg/events`: Kafka event types, topic names and the Avro schemas (`schemas/*.avsc`)
- `services/pkg/eventcodec`: Avro encode/decode with Schema Registry (registers schemas, resolves writer vs reader schema)
- `services/pkg/kafkaconsumer`: the retry + dead-letter consumer loop
- `services/pkg/driverstatus`: the Redis driver status and vehicle keys, and the atomic claim (AVAILABLE and the right vehicle, or nothing)
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
- **A port is already in use** (3001-3003, 5171-5172, 5432, 6379, 8080, 8081, 9092): stop whatever holds it. Vite silently moves to the next port if its own is taken, which breaks the URLs above; `lsof -nP -iTCP:5171 -sTCP:LISTEN` shows the culprit.
- **The rider stays on "Looking for a driver"**: check `.dev/logs/matching-service.log` and that the drivers are seeded (`docker exec grab-mini-redis redis-cli GEOPOS drivers:geo driver-1`).
- **The driver app shows no trip**: it must be open, as the matched driver, when the trip is assigned. Reload doesn't recover an in-flight trip.
- **Start from scratch**: `scripts/dev.sh --reset`.
