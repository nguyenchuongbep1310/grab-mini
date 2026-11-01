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

1. In the **Rider App**, pick **Car** or **Bike**, tap the map to drop a destination pin, then **Book ride**. Next to the price the app shows the route length and an estimated time ("3.2 km · about 8 min"), and after the match it shows how long until the driver arrives, then until you reach the destination (from Routing Service, a model without real map data; if that service is down the line is simply hidden).
2. The rider is matched with the nearest driver of that type and can watch them move on the map. Five drivers are seeded around central Ho Chi Minh City: `driver-1` to `driver-3` drive cars, `driver-4` and `driver-5` drive bikes. They also have approved profiles (`profile-service seed-dev`, development only; names, plates and vehicles are invented, and `rider-1` to `rider-3` have profiles too), so the matched card in the Rider App shows the driver's name, vehicle (model, colour, type) and plate instead of the id (asked once per driver; if the lookup fails or the driver has no approved profile, the card shows the id as before and the ride is not affected), and the avatar in the header opens **Your profile**, where the rider edits their name and phone (the phone is saved as typed and is not verified; the avatar shows the name's initials); a driver who is not approved is not offered trips, but a driver put straight into Redis, as the load and end-to-end tests do, is matched as before. Three demo promotion codes are seeded too (`promotion-service seed-dev`, development only): `WELCOME10` (10% off, at most 20,000 VND, fare 40,000 or more, once per rider), `FLAT15K` (15,000 VND off a fare of 40,000 or more, 5 times per rider) and `BIKE5K` (5,000 VND off a BIKE trip). To use one, tap **Have a promo code?** under the price in the Rider App (it appears once the price is shown), type `WELCOME10` and **Apply**: the app shows what it takes off and the new total, and the code is locked onto the order when you book. A code that does not fit the trip (for example a fare below its minimum, or `BIKE5K` on a car) is refused with the reason, and if you change the destination or vehicle the code is checked again for the new price. The receipt after the trip shows the fare, the promo line and what was charged. You can also try the codes with the API calls under "Promotion Service" below.
3. Open the **Driver App as that driver**, e.g. `http://localhost:5172/?driver=driver-2`. **Confirm pickup**, then **Complete trip**, and the rider's timeline follows along.
4. Back in the **Rider App**, rate the driver (1 to 5 stars) and submit. You can also add a comment and a tip (0, 10k, 20k, 50k or custom VND). A tip is only recorded, there is no payment system: Rating Service keeps a per-driver total and the driver's app shows it in the toast. The driver's app shows a toast and their updated average.
5. While the trip is on (driver assigned, or in progress), both apps show a **Chat** button. Tap it in the Rider App and in the Driver App, type a message and press Enter (or **Send**): the other side gets a badge with the number of unread messages on its Chat button, and the message appears when it opens the sheet. Messages are at most 500 characters on one line. A message that could not be sent shows "Failed - retry" (the retry cannot send it twice), sending too fast pauses the button for a few seconds, and when the server closes the chat (10 minutes after the trip ends) the sheet becomes read-only with "This chat has ended". The apps reach Chat Service through the `/chat-api` proxy (port 3011) and receive the other person's messages as `chat.message.sent` pushes on the same WebSocket; if the socket drops, the history is reloaded when it reconnects.
6. If you reload the Rider App (or the "trip completed" push was missed), a ride that is still under way comes back as before, and so does a ride that finished in the last 24 hours and that you have not rated or closed yet: you see its receipt and the rating prompt again. Once you rate it or tap **New ride**, a reload starts from the booking screen (a cancelled or no-driver ride is never brought back).

Also try: rating a trip twice (the second is rejected), booking a bike (only `driver-4`/`driver-5` can take it), **Cancel trip** from either side, or booking when every driver is busy (you get "No drivers nearby" after the matching service's retries). `?rider=rider-2` picks a different rider.

The map background comes from OpenStreetMap tiles, so it needs internet access; markers work without it.

### Become a driver

Anyone can apply; an admin decides. Try it with a driver id that has no profile:

1. Open the Driver App as a new driver, `http://localhost:5172/?driver=driver-9`. Instead of "You're online" it shows **Become a driver**: fill in the name, phone (optional), vehicle type, plate (e.g. `59-X1 123.45`), and optionally model and colour, then **Submit application**. Each field shows its own error if it is refused.
2. The app now says **Waiting for review** and checks again every 10 seconds. No trips are shown while the driver is not approved.
3. Open the [Ops dashboard](http://localhost:5173), scroll to **Driver applications** (below Promotions): the application is listed with the name, vehicle, plate and when it was sent. Click **Approve** then **Confirm approve driver-9**. (**Reject** asks for a reason, which the driver reads; the driver app then shows it with a **Fix and re-apply** form.)
4. Within 10 seconds the Driver App unlocks and shows "You're online". An approved driver can edit the model and colour from the avatar button; changing the **plate or vehicle type** sends the profile back to review (no trips until an admin approves it again). **Suspend a driver** in the same panel takes an approved driver out of service with a reason, shown to them; the panel's **Suspended** list has **Reinstate**.

The driver app does not tell you "approved" if the profile service is unreachable: an existing driver sees the normal app with a small notice, and a refusal (403 or 404) shows "Can't verify your driver account". The real gate is Matching Service, which offers trips only to approved drivers ([production-design §20](../specs/ride-booking-mini/production-design.md)).

## Watching Kafka work

Open [Kafka UI](http://localhost:8080), cluster **grab-mini**, while you take a ride:

- **Topics**: `order.created`, `order.matched`, `order.no_driver_found`, `trip.status.changed` and `driver.location.updated`, each with 3 partitions. Open a topic's **Messages** tab: Kafka UI decodes the Avro values through Schema Registry, so you see JSON. `driver.location.updated` grows every few seconds.
- **Partition keys**: messages for one order (or one driver) always land in the same partition, which keeps their order. Compare the **Partition** column across messages.
- **Schema Registry** (menu item in Kafka UI, or `curl localhost:8081/subjects`): one subject per topic (`order.created-value`, ...), each holding the Avro schema producers registered.
- **Consumers**: one consumer group per service (`matching-service-group`, `trip-service-group`, `driver-location-service-group`, `notification-service-group`). Each reads every event on its topics independently. Check **lag** here: it stays at or near 0 (a message or two in flight on `driver.location.updated` while the simulator publishes; `N/A` just means a group hasn't committed an offset on that topic yet).

## Analytics with ksqlDB

The [Ops dashboard](http://localhost:5173) shows live figures (orders per minute by vehicle type, match rate, average match time, trips completed, driver ratings, location update throughput). Below them a **Promotions** panel is where an admin manages promo codes: the list shows each code's discount, who it applies to, validity, usage against its limits and status, **New promotion** creates one, and **Disable** stops new uses. It talks to Promotion Service, not ksqlDB, and signs in as an admin through the development token issuer (`?admin=<id>` picks the admin id, default `ops-1`). Below it, **Driver applications** is the review queue (Profile Service): drivers waiting for review with their name, vehicle, plate, the phone as typed (not verified) and when they applied, oldest first (**Load more** pages through the cursor), **Approve** and **Reject** (a reason is required and shown to the driver, so keep personal data about other people out of it), a form to **Suspend** an approved driver by id with a reason, and a **Suspended** view with **Reinstate**. Every decision asks for a second click, the list refreshes afterwards, and a decision that no longer fits (another admin was first) says which status the driver has now. None of the figures above is computed by a service we wrote: [ksqlDB](https://ksqldb.io) reads the Avro topics and maintains the tables in [ksql/statements.sql](../ksql/statements.sql), and the dashboard just runs pull queries against them (through the ksqlDB REST API).

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
  cd services/matching-service && APP_ENV=development INSTANCE_ID=matching-B go run .
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
  cd services/rating-service && APP_ENV=development go run .
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
| Matching Service | | `order.created`, `order.cancelled`, `driver.application.reviewed` (who may be offered trips) | `order.matched`, `order.no_driver_found` | Redis (driver status + GEO; `driver:<id>:suspended` for a driver an admin took out; [production-design §20](../specs/ride-booking-mini/production-design.md)) |
| Trip Service | 3002 | `order.matched` | `trip.status.changed`, `trip.rated` | Postgres, Redis (releases drivers) |
| Driver Location Service | | `driver.location.updated` | | Redis GEO |
| Rating Service | 3004 | `trip.rated` | | Postgres (per-driver totals) |
| ksqlDB | 8088 | every topic above | analytics tables (`analytics` section) | Kafka (changelog topics) |
| Notification Service | 3003 | all of the above | | WebSocket connections (per instance), Redis (routing state and Pub/Sub between instances) |
| Auth Service | 3005 | | | none (development token issuer) |
| Pricing Service | 3007 | `order.created` (demand per pickup cell) | | Redis (surge multipliers, demand counters); quotes are signed, not stored; asks Routing Service for the distance (`ROUTING_URL`), falling back to the straight line |
| Routing Service | 3009 | | | none (stateless; a deterministic distance and travel-time model, no maps data: [production-design §17](../specs/ride-booking-mini/production-design.md)) |
| Promotion Service | 3010 | `order.created`, `order.cancelled` (to record and release redemptions) | `promotion.redeemed` | Postgres (promotions, redemptions, outbox); validation signs a short-lived `promoToken` that Order Service checks without calling it ([production-design §18](../specs/ride-booking-mini/production-design.md)) |
| Chat Service | 3011 | `trip.status.changed` (who is on a trip, and when it ended), or a lookup in Trip Service on a miss | `chat.message.sent` | Postgres (chat_trips, chat_messages, outbox); messages are personal data, never logged, deleted after `CHAT_RETENTION_DAYS` ([production-design §19](../specs/ride-booking-mini/production-design.md)) |
| Profile Service | 3012 | | `driver.application.reviewed` | Postgres (rider_profiles, driver_profiles, outbox); names, phones and plates are personal data, never logged ([production-design §20](../specs/ride-booking-mini/production-design.md)) |
| Payment Service | 3008 | `order.created` (the locked price and discount), `trip.status.changed`, `trip.rated` | `payment.recorded` | Postgres (double-entry ledger, payments); a sandbox `PaymentProvider`, no real card processor ([production-design §16](../specs/ride-booking-mini/production-design.md)) |
| Location Gateway | 3006 | | `driver.location.updated` | per-driver rate limit in memory |
| `driver-app-sim` | | | | reports positions to the Location Gateway over HTTP |

Order Service and Trip Service never publish straight from a request: the event is saved in an `outbox` table in the same Postgres transaction as the order, trip status or rating, and a relay ([services/pkg/outbox](../services/pkg/outbox)) publishes it to Kafka afterwards, so a crash can't leave a state change nobody was told about. Matching Service records which driver it gave each order (`match:<orderId>` in Redis), so a redelivered `order.created` can't claim a second driver, and a claimed driver is only *leased* (`ASSIGNED` for `ASSIGN_LEASE_SECONDS`, default 120) until Trip Service confirms the trip and sets them `ON_TRIP`. Trip Service also frees the driver from the `trip.status.changed` event (a safety net for a crash between saving the trip and the Redis update). Every service stops cleanly on SIGTERM / Ctrl-C.

A message a consumer can't handle right now (a database that is down) is not retried in place: it moves to `<topic>.retry.1`, then `.2`, `.3` (waiting 5s, 1m, 10m), so the partition keeps flowing; after the last tier, or at once for a message that can never succeed, it lands on `<topic>.DLQ`. Notification Service can run as several instances: events are consumed by any of them, and each message is published to a Redis channel for its recipient so the instance holding that socket delivers it.

### Security

Every API call and every WebSocket carries a JWT, and the services trust the token, not the request: a rider's id comes from their token (a body naming someone else is a 403), a rider sees only their own orders and trips, only a trip's driver can pick up or complete it, and only its rider can rate it. Tokens are Ed25519 (EdDSA) JWTs: only the issuer holds the private key and the services verify with its public keys from `GET /.well-known/jwks.json` (`JWT_JWKS_URL`), so no service can mint a token; see [security.md](security.md). In development (`APP_ENV=development`, which `scripts/dev.sh` sets) [Auth Service](../services/auth-service) signs a token for whoever asks (`curl -X POST localhost:3005/token -d '{"role":"rider","id":"rider-1"}'`; the role is `rider`, `driver` or `admin`, the last for the operators' Ops dashboard and admin-only routes), which is why it refuses to start with `APP_ENV=production`; the apps sign in through `@grab-mini/client`. A WebSocket sends its token as a subprotocol (not in the URL) and is only accepted from an origin in `ALLOWED_ORIGINS`.

Drivers' phones never talk to Kafka. [Location Gateway](../services/location-gateway) authenticates the driver, validates the position, limits each driver to one report a second, skips a parked driver's reports, and publishes in batches.

Credentials come from `NAME_FILE` (a mounted secret) or `NAME`, and with `APP_ENV=production` a missing one is an error rather than a development default: `DATABASE_URL`, `KAFKA_PASSWORD`, and (for the issuer only) `JWT_PRIVATE_KEY`; verifiers need `JWT_JWKS_URL` or `JWT_PUBLIC_KEY_FILE`. The signing keys, `JWT_PRIVATE_KEY` and `QUOTE_SECRET`, are stricter: their published development values are used only with `APP_ENV=development` (so a deployment that forgets `APP_ENV` fails instead of running with them), and are refused anywhere else even when set explicitly; a verifier likewise refuses to trust the development public key outside development. The Schema Registry takes `SCHEMA_REGISTRY_USER` / `SCHEMA_REGISTRY_PASSWORD` (or `_FILE`). For Kafka, set `KAFKA_SECURITY_PROTOCOL=SASL_SSL`, `KAFKA_USERNAME`, `KAFKA_PASSWORD_FILE` and `KAFKA_CA_FILE` (see [pkg/kafkaconn](../services/pkg/kafkaconn)); production refuses a plaintext connection. Each service has its own principal and `scripts/kafka-acls.sh` grants it only what it does (the table in that script is the source of truth). To try a Kafka with TLS, SASL/SCRAM and ACLs without Docker:

```bash
KAFKA_HOME=/path/to/kafka_2.13-3.8.0 deploy/kafka-secure/start-local.sh   # prints the environment to give a service
```

`docker compose` still starts an unauthenticated PLAINTEXT Kafka for development.

Each service owns its schema as versioned SQL in `services/<service>/migrations` (goose, a version table per service). They are applied at startup; `<service> migrate` applies them and exits, and `RUN_MIGRATIONS=false` skips them at startup. Every service with an HTTP API serves `/livez` (alive) and `/readyz` (its Postgres, Redis and Kafka answer; 503 once shutdown begins); Matching and Driver Location have no API, so set `HEALTH_PORT` to get the same two probes.

Every consumer runs through [services/pkg/kafkaconsumer](../services/pkg/kafkaconsumer): retry, then dead-letter topic, and uniform logging of topic/partition/offset. Every producer and consumer encodes and decodes through [services/pkg/eventcodec](../services/pkg/eventcodec) (Avro plus the Schema Registry wire format); the schemas live in [services/pkg/events/schemas](../services/pkg/events/schemas). Duplicate events are harmless (dedupe by `eventId`, idempotent state transitions).

`driver-app-sim` publishes the drivers' locations because a browser can't produce to Kafka directly; the Driver App only displays its position.

To run a single piece by hand instead of `pnpm dev:all` (with `docker compose up -d` already running):

```bash
export APP_ENV=development                              # lets the services use the public development signing keys
cd services/matching-service && go run . seed-drivers   # once, to seed drivers
cd services/order-service && go run .                   # likewise for each service
```

Without `APP_ENV=development` (or a real `JWT_PRIVATE_KEY` for the issuer, `JWT_JWKS_URL` or `JWT_PUBLIC_KEY_FILE` for the services that check tokens, and a real `QUOTE_SECRET`), the services that sign or check tokens and quotes refuse to start: the development keys are in this repository, so they are never used by accident.

### HTTP API

```bash
# Order Service
curl -X POST localhost:3001/orders -H "Content-Type: application/json" \
  -d '{"riderId": "rider-1", "pickup": {"lat": 10.776, "lng": 106.700}, "dropoff": {"lat": 10.79, "lng": 106.71}, "vehicleType": "BIKE"}'   # vehicleType: CAR (default) or BIKE
curl localhost:3001/orders/<orderId>                  # includes status: PENDING, then MATCHED or NO_DRIVER
curl localhost:3001/riders/<riderId>/latest-order     # what a reloaded Rider App uses to find its ride

# Trip Service
curl localhost:3002/trips/by-order/<orderId>
curl localhost:3002/trips/<tripId>                    # a COMPLETED trip also has completedAt (RFC 3339); empty for the others
curl localhost:3002/drivers/<driverId>/active-trip    # the driver's ASSIGNED or IN_PROGRESS trip, else 404
curl -X POST localhost:3002/trips/<tripId>/pickup     # ASSIGNED -> IN_PROGRESS
curl -X POST localhost:3002/trips/<tripId>/complete   # IN_PROGRESS -> COMPLETED (answers with completedAt), driver becomes AVAILABLE
curl -X POST localhost:3002/trips/<tripId>/cancel     # from ASSIGNED or IN_PROGRESS; 409 once COMPLETED
curl -X POST localhost:3002/trips/<tripId>/rating -H "Content-Type: application/json" \
  -d '{"riderId": "rider-1", "stars": 5, "comment": "Smooth ride", "tipAmount": 20000}'   # COMPLETED trips only, once (409 otherwise); tipAmount is optional whole VND, 0 to 1,000,000

# Rating Service
curl localhost:3004/drivers/driver-1/rating          # {"average": 4.67, "count": 3, "totalTips": 30000, "tipCount": 2}
curl "localhost:3004/drivers/driver-1/reviews?limit=20&before=<createdAt>"   # a driver's own reviews, newest first (driver token only)

# Routing Service (any valid token: rider, driver or admin). Distance by road (great-circle x 1.3) and travel time for the hour in Vietnam
curl "localhost:3009/route?fromLat=10.776&fromLng=106.700&toLat=10.790&toLng=106.710&vehicle=BIKE" -H "Authorization: Bearer <token>"   # {"distanceKm": 2.47, "durationSec": 405, "polyline": [{"lat":...,"lng":...}, ...], "vehicle": "BIKE", "model": "detour-1.3"}; vehicle is CAR (default) or BIKE; 400 for bad input, 422 beyond 200 km

# Promotion Service. Riders check a code for a price (whole VND) and vehicle; admins manage promotions (an admin token: {"role":"admin","id":"ops-1"})
curl "localhost:3010/promotions/validate?code=WELCOME10&price=87000&vehicleType=CAR" -H "Authorization: Bearer <rider token>"   # {"code":"WELCOME10","discount":8700,"price":87000,"finalPrice":78300,"promoToken":"...","expiresAt":...}; otherwise 404/422 {"error":"...","code":"not_found|not_started|expired|min_fare|wrong_vehicle|limit_reached|already_used"}
curl -X POST localhost:3001/orders -H "Authorization: Bearer <rider token>" -H "Content-Type: application/json" \
  -d '{"pickup":{"lat":10.776,"lng":106.700},"dropoff":{"lat":10.790,"lng":106.710},"promoToken":"<promoToken>"}'   # locks the discount: {"orderId":...,"price":87000,"discount":8700,"finalPrice":78300,"promoCode":"WELCOME10"}; a stale or foreign token is 400 {"code":"promo_expired|promo_wrong_price|promo_wrong_rider|promo_wrong_vehicle|promo_invalid"}, a used one 409 promo_used
curl localhost:3010/promotions -H "Authorization: Bearer <admin token>"   # every promotion with how often it was redeemed (redeemed, overLimit)
curl -X POST localhost:3010/promotions -H "Authorization: Bearer <admin token>" -H "Content-Type: application/json" \
  -d '{"code":"SUMMER20","kind":"PERCENT","value":20,"maxDiscount":30000,"minFare":50000,"vehicleType":"CAR","validTo":"2027-01-31T00:00:00Z","usageLimit":100,"perRiderLimit":2}'   # kind PERCENT or FIXED; only code, kind, value and validTo are required
curl -X POST localhost:3010/promotions/SUMMER20/disable -H "Authorization: Bearer <admin token>"   # stops new uses; orders that carry its discount keep it

# Chat Service (rider or driver token; only the rider and the driver of that trip. The chat opens when the trip is assigned and stays writable for 10 minutes after it ends)
curl -X POST localhost:3011/trips/<tripId>/messages -H "Authorization: Bearer <rider or driver token>" -H "Content-Type: application/json" -H "Idempotency-Key: <any unique string>" \
  -d '{"text":"I am at the gate"}'   # 201 {"id":...,"tripId":...,"senderId":...,"senderRole":"rider","text":"I am at the gate","createdAt":...}; 1 to 500 characters, no control characters (400 invalid_text); 403 not_participant, 404 unknown_trip, 503 trip_lookup_unavailable (Trip Service could not be asked about a trip chat had not learned yet), 409 chat_closed, 429 rate_limited (Retry-After); the same Idempotency-Key and text again returns the saved message (200) instead of sending twice
curl "localhost:3011/trips/<tripId>/messages?limit=20" -H "Authorization: Bearer <rider or driver token>"   # {"messages":[newest first],"nextBefore":"...","tripStatus":"ASSIGNED","open":true,"closesAt":null}; pass nextBefore as ?before= for older ones. The other person gets a "chat.message.sent" push on their WebSocket

# Profile Service (rider and driver tokens act on their own id only; a different id is 403). `scripts/dev.sh` seeds approved profiles for driver-1..5 and profiles for rider-1..3
curl -X PUT localhost:3012/riders/rider-1 -H "Authorization: Bearer <rider token>" -H "Content-Type: application/json" \
  -d '{"name":"Hoàng Gia Bảo","phone":"0900 100 001"}'   # name 1 to 80 characters (required); phone optional, 7 to 15 digits; 400 {"code":"invalid_profile","field":"phone"}; GET /riders/rider-1 reads it back (empty, not 404, before the first save)
curl -X PUT localhost:3012/drivers/driver-9 -H "Authorization: Bearer <driver token>" -H "Content-Type: application/json" \
  -d '{"name":"Lê Minh","phone":"0900 000 009","vehicleType":"BIKE","plate":"59-x1 123.45","model":"Honda Wave","color":"Xanh"}'   # saves a draft (status NONE); the plate is stored in capitals. Changing the plate or vehicle type of an APPROVED driver puts them back to PENDING and out of service until an admin approves
curl -X POST localhost:3012/drivers/driver-9/application -H "Authorization: Bearer <driver token>"   # 201 status PENDING; again, or when APPROVED: 200 unchanged; 409 suspended; 422 incomplete_profile (name, vehicleType and plate are needed)
curl "localhost:3012/driver-applications?status=PENDING&limit=20" -H "Authorization: Bearer <admin token>"   # {"applications":[oldest first, with phone],"nextCursor":"..."}; pass nextCursor as ?cursor=
curl -X POST localhost:3012/drivers/driver-9/approve -H "Authorization: Bearer <admin token>"   # PENDING or SUSPENDED to APPROVED; Matching then makes the driver AVAILABLE (if not on a trip). The driver must also send locations
curl -X POST localhost:3012/drivers/driver-9/suspend -H "Authorization: Bearer <admin token>" -H "Content-Type: application/json" -d '{"reason":"Complaint 4412"}'   # APPROVED to SUSPENDED; reject (from PENDING) and suspend need a reason of 1 to 500 characters; any other transition is 409 invalid_transition with the current status. A driver on a trip finishes it and is then offered no more
curl localhost:3012/drivers/driver-1/public -H "Authorization: Bearer <any token>"   # {"driverId":...,"name":...,"vehicleType":"CAR","plate":"51A-123.45","model":...,"color":...} for an APPROVED driver, never the phone; 404 for anyone else

# Payment Service (tokens as above; a rider sees what they were charged, a driver what they earn)
curl "localhost:3008/payments?tripId=<tripId>" -H "Authorization: Bearer <token>"   # fare, tip and cancellation fee of one trip, plus the captured total
curl localhost:3008/cancellation-policy -H "Authorization: Bearer <token>"   # {"currency":"VND","fee":10000,"graceSeconds":180}: what cancelling after a driver is assigned costs, and how long it is free
curl localhost:3008/drivers/driver-1/earnings -H "Authorization: Bearer <driver token>"   # {"total": 98000, "fares": 80000, "tips": 18000, "commission": 20000, "trips": 1, ...}; optional ?from=&to= (RFC 3339)
```

Repeating `pickup` or `complete` is a no-op. The WebSocket is `ws://localhost:3003/ws/rider/<id>` or `/ws/driver/<id>`, pushing `{"type": "...", "payload": {...}}`.

Inspect state directly:

```bash
docker exec grab-mini-redis redis-cli GEOPOS drivers:geo driver-1
docker exec grab-mini-redis redis-cli GET "driver:driver-1:status"
docker exec grab-mini-redis redis-cli GET "driver:driver-1:vehicle"
docker exec grab-mini-postgres psql -U grabmini -d grabmini -c "select * from trips;"
```

**Retries.** `POST /orders` accepts an optional `Idempotency-Key` header (1 to 128 printable characters; `@grab-mini/client` sends a fresh UUID per booking and reuses it when it retries after a network error or 5xx). Repeating a key with the same body returns the original response (same `orderId`, header `Idempotent-Replayed: true`) and publishes nothing; the same key with a different body is `422`. Keys are scoped to the rider and kept for 24 hours. A `quoteId` books once: using it again is `409` with a fresh `newQuote` (if Redis is down the booking is priced at the current multiplier instead of being rejected).

**Pickup and complete need the driver to be there.** `pickup` is refused with `409` unless the driver's last reported location is within `PICKUP_RADIUS_M` (default 200) of the pickup point, `complete` within `DROPOFF_RADIUS_M` (default 300) of the dropoff. The error `code` says why: `driver_too_far`, `driver_location_unknown`, or `driver_location_stale` (older than `LOCATION_MAX_AGE`, default 60s); if Redis is unreachable it is a `503`. Repeating a step that already happened is still a no-op. `driver-app-sim` only wanders near its seed point, so `make dev` and `deploy/loadtest/stack.sh` start Trip Service with `APP_ENV=development PROXIMITY_CHECK=off`; Trip Service refuses `PROXIMITY_CHECK=off` under any other `APP_ENV`. To try the check locally, unset it and report a position first (`POST localhost:3006/locations` with a driver token) or use `redis-cli`/`driverlocation.Update`.

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
   - Log in as `admin` (password `admin`, or `GRAFANA_ADMIN_PASSWORD` if set when starting Compose). Anonymous access is off.
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
- **A port is already in use** (3000-3009, 5170-5173, 5432, 6379, 8080, 8081, 8088, 9090, 9092, 9101, 9102): stop whatever holds it. Vite silently moves to the next port if its own is taken, which breaks the URLs above; `lsof -nP -iTCP:<port> -sTCP:LISTEN` shows the culprit.
- **The rider stays on "Looking for a driver"**: check `.dev/logs/matching-service.log` and that the drivers are seeded (`docker exec grab-mini-redis redis-cli GEOPOS drivers:geo driver-1`).
- **The rider app is on a screen that doesn't match the trip**: reload it. It asks Order Service for the rider's latest order and Trip Service for its trip, and resumes searching, assigned or in-progress rides (a missed WebSocket push while searching is also caught by polling every 3s).
- **The driver app shows no trip**: check the `?driver=` id matches the driver the rider was matched with (the rider's card shows it). A trip already under way is restored when the page opens or reloads.
- **The Ops dashboard says ksqlDB is unreachable, or shows no data**: ksqlDB is a JVM and takes a minute to start; `docker logs grab-mini-ksqldb` shows its progress, and `docker logs grab-mini-ksql-init` shows whether the tables were created. The tables fill as events flow, so book a ride first.
- **Start from scratch**: `make dev-reset` (or `scripts/dev.sh --reset`).

