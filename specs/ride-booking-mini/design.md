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
| Order Service | Accepts booking requests, manages orders, keeps each order's status current; locks the price and, when the booking carries a verified promo token, the discount | `order.matched`, `order.no_driver_found` | `order.created` | Postgres |
| Pricing Service | Surge pricing: counts demand per pickup cell, samples supply, keeps a multiplier per cell, issues signed price quotes (`GET /quotes`) | `order.created` | | Redis (demand counters, multipliers); no Postgres |
| Routing Service | Estimates the road distance, travel time and a simple polyline between two points (`GET /route`); a deterministic model (great-circle distance x 1.3, speed by vehicle and hour of day), no map data | | | none (stateless: no Kafka, Postgres or Redis) |
| Matching Service | Finds the nearest driver; applies Profile Service's review results: an approved driver becomes eligible (`AVAILABLE` with the vehicle type, unless on an order or trip), a rejected or suspended one is taken out (a driver on a trip finishes it first) | `order.created`, `order.cancelled`, `driver.application.reviewed` | `order.matched`, `order.no_driver_found` | Redis (driver status/geo, `driver:{id}:suspended`) |
| Driver Location Service | Receives & stores driver location | `driver.location.updated` | — | Redis (geo, TTL) |
| Trip Service | Trip state machine | `order.matched` | `trip.status.changed` | Postgres |
| Notification Service | Pushes realtime updates to clients (a chat message goes to the other person on the trip only) | `order.matched`, `order.no_driver_found`, `trip.status.changed`, `driver.location.updated`, `chat.message.sent` | — | (stateless, in-memory socket map) |
| Promotion Service | Promotions (vouchers): validates a code for a rider, price and vehicle and signs a short-lived `promoToken` (`GET /promotions/validate`); admins create, list and disable promotions; records each order's redemption and releases it when the order ends without a ride (cancelled while pending, trip cancelled after the match, no driver) | `order.created`, `order.cancelled`, `trip.status.changed`, `order.no_driver_found` | `promotion.redeemed` | Postgres (promotions, redemptions, outbox) |
| Chat Service | Text chat between the rider and the driver of a trip: learns who is on a trip, and when it ended, from `trip.status.changed`; `POST /trips/{id}/messages` and `GET /trips/{id}/messages` for the two participants only; messages are personal data and are deleted after `CHAT_RETENTION_DAYS` | `trip.status.changed` | `chat.message.sent` | Postgres (`chat_trips`, `chat_messages`, outbox) |
| Profile Service | Rider profiles (name, phone) and driver profiles (name, phone, vehicle, plate) with the driver's onboarding status (`NONE`, `PENDING`, `APPROVED`, `REJECTED`, `SUSPENDED`); a driver applies, an admin approves, rejects or suspends; a public view (name, vehicle, plate, never the phone) of an approved driver for any signed-in user; names, phones and plates are personal data and are never logged | | `driver.application.reviewed` | Postgres (`rider_profiles`, `driver_profiles`, outbox) |
| Payment Service | Settles rides: charges the rider (price less any promo discount) through a pluggable provider (sandbox fake only) and books a balanced double-entry ledger entry for the fare, tip and optional cancellation fee | `order.created` (local copy of the locked price), `trip.status.changed`, `trip.rated` | `payment.recorded` | Postgres (ledger, payments, outbox) |
| Rating Service | Averages each driver's ratings | `trip.rated` | | Postgres (`driver_ratings` with tip totals, `driver_reviews`, deduped by eventId in the same transaction) |
| ksqlDB (replaces the planned Analytics Service) | Stream processing: per-minute counts, a stream-stream join for match latency, ratings, throughput | all topics | analytics tables | Kafka (changelog topics) |

## 3. Kafka topics

| Topic | Partition key | Partitions (local) | Notes |
|---|---|---|---|
| `order.created` | `orderId` | 3 | Avro v4: also carries `promoCode` (default `""`) and `discount` (default 0, whole VND), both locked at booking. See section 4f |
| `order.matched` | `orderId` | 3 | |
| `order.no_driver_found` | `orderId` | 3 | |
| `order.cancelled` | `orderId` | 3 | Published by Order Service (via the outbox) when a rider cancels a `PENDING` order. Fields: `riderId`, `cancelledBy`, `reason` (all defaulted, so the schema stays backward compatible). Consumed by Matching (Redis key `order:{id}:cancelled`, 24h), Trip Service (`trip-order-cancelled-group`) and Notification |
| `driver.location.updated` | `driverId` | 3 | Highest throughput; keyed by driver to preserve per-driver ordering |
| `trip.status.changed` | `tripId` | 3 | On `CANCELLED` also carries `cancelledBy` (`RIDER`/`DRIVER`) and `reason`; Order Service consumes it to move a `MATCHED` order to `CANCELLED` |
| `trip.rated` | `driverId` | 3 | Published by Trip Service when a rider rates a COMPLETED trip (once per trip); carries `comment`, `tipAmount` (whole VND, 0 to 1,000,000, recorded only) and `currency` (`VND`), both added as defaulted Avro fields |
| `payment.recorded` | `tripId` | 3 | Published by Payment Service (via the outbox) for every charge it records: fare, tip or cancellation fee, captured or declined. Amounts are whole VND; the split between driver and platform is included. See [production-design.md](./production-design.md#16-payments-payment-service-done) |
| `promotion.redeemed` | `orderId` | 3 | Published by Promotion Service (via the outbox) once for each `order.created` that carried a `promoCode`: `code`, `discount`, `riderId` and `status` (`REDEEMED`, or `OVER_LIMIT` when a limit had been reached by the time the order was recorded). For analytics; nothing waits for it. See section 4f |
| `chat.message.sent` | `tripId` | 3 | Published by Chat Service (via the outbox) for every message: `messageId`, `tripId`, `senderId`, `senderRole`, `recipientId`, `recipientRole` (`rider` or `driver`) and `text`. Notification Service pushes it to the recipient. The text is personal data. See section 4g |
| `driver.application.reviewed` | `driverId` | 3 | Published by Profile Service (via the outbox, in the transaction that changes the status) for every admin decision, and when an approved driver changes plate or vehicle type: `driverId`, `status` (`APPROVED`, `REJECTED` or `SUSPENDED`), `vehicleType`, `reason`, `reviewedBy` (`system` for the vehicle change). `occurredAt` strictly increases per driver, so Matching ignores an older result that arrives late. No name, phone or plate. See section 4h |
| `*.DLQ` | same as source topic | 1 | Dead-letter topic per topic above |

Consumer group naming: `<service-name>-group` (e.g. `matching-service-group`) so each service scales independently without reading each other's events.

## 4. Data model (simplified)

**Order** (Postgres)
```
id, riderId, pickupLat, pickupLng, dropoffLat, dropoffLng, vehicleType[CAR|BIKE], status[PENDING|MATCHED|NO_DRIVER|CANCELLED], surgeMultiplier, price, promoCode, discount, createdAt, cancelledAt, cancelledBy, cancelReason
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
driver:{id}:suspended -> 1, while Profile Service has taken the driver out of service (no expiry)
driver:{id}:review_at -> unix microseconds of the last review result applied
```
(`OFFLINE` is the status of a driver who is not eligible. Every claim refuses a driver with a `suspended` key, and giving a driver back to `AVAILABLE` after a trip records `OFFLINE` for one.)

**Driver profile** (Postgres, Profile Service)
```
driver_id, name, phone, vehicle_type[CAR|BIKE], plate, model, color, status[NONE|PENDING|APPROVED|REJECTED|SUSPENDED], reason, reviewed_by, reviewed_at, status_at, updated_at
rider_profiles: rider_id, name, phone, updated_at
```

## 4a. Surge pricing

- **Cells.** A pickup falls in the cell `floor(lat/deg):floor(lng/deg)`, the same grid as the driver GEO shards (`GEO_SHARD_DEGREES`; `SURGE_CELL_DEGREES` overrides; default 0.05 degrees, about 5.5 km). The pure functions live in `services/pkg/surge`.
- **Demand.** Pricing Service consumes `order.created` (group `pricing-service-group`) and does `INCR surge:demand:{vehicle}:{cell}:{minute}` (TTL 5 minutes). Active cells are remembered in the set `surge:cells`.
- **Supply and multiplier.** Every 5 seconds one instance (a short Redis lock) takes each active cell: supply = AVAILABLE drivers of that vehicle type within the equal-area radius of the cell centre (`driverlocation.Search` plus `driverstatus.AvailableVehicles`), demand = average orders per minute over the last 3 minute buckets, `target = clamp(1 + k * (demand / max(supply, 1) - 1), 1.0, MAX)` rounded to 0.1 (k = 0.5, MAX = 3.0), then smoothed with an EMA and a per-tick change cap (0.3) so prices do not jump. The result is `SET surge:mult:{vehicle}:{cell}` with a 30 second TTL; a missing key means 1.0.
- **Quote.** `GET /quotes?pickupLat&pickupLng&dropoffLat&dropoffLng&vehicleType` (Rider token) returns `{quoteId, baseFare, multiplier, price, currency: "VND", expiresAt}`. Base fare is a flag fall plus a per-km rate on the distance, with a minimum; the distance is the road distance from Routing Service (`ROUTING_URL`, 300 ms timeout) and falls back to the straight line if it cannot be had (counted in `pricing_routing_fail_open_total`, alert `PricingRoutingFailOpen`). `quoteId = base64url(body) "." base64url(HMAC-SHA256)`; the body holds the fare, multiplier, price, expiry and a fingerprint of rider, vehicle, pickup and dropoff, so nothing is stored. Pricing Service and Order Service share `QUOTE_SECRET` (`pkg/secret`: file or env, no dev default in production).
- **Booking.** `POST /orders` takes an optional `quoteId`: valid means the quoted price is locked; absent or expired means the multiplier is read from Redis and the price locked at booking; an expired quote whose multiplier moved by more than `SURGE_QUOTE_TOLERANCE` (0.2) gets `409 {newQuote}` for the rider to confirm. The order stores `surge_multiplier` and `price` (migration 00006) and `order.created` v3 carries `surgeMultiplier` (default 1.0) and `price` (nullable), so older messages and consumers still work.
- **Failing open.** Redis errors, timeouts or an absent key all price at 1.0 (counted in `pricing_fail_open_total` / `orders_surge_fail_open_total`, alert `SurgePricingFailOpen`). A stopped sampler lets multipliers expire after 30 seconds (alert `SurgeSamplerStalled`).
- **Frontend.** `getQuote()` in `packages/client`, the `useQuote` hook in the Rider App (debounced refetch, refresh before expiry, re-confirm after a 409), and `PriceTag` / `SurgeBadge` in the design system. Ops shows the `surge_demand` ksqlDB table.

## 4b. Event serialization

Every event is Avro in Confluent's Schema Registry wire format (magic byte, 4-byte schema id, Avro payload). Schemas live in `services/pkg/events/schemas/*.avsc`, one per topic, registered under the subject `<topic>-value`. The registry enforces `BACKWARD` compatibility, so a new schema version must be able to read data written with the old one; in practice, new fields need a default. Consumers read the writer's schema id from each message and resolve it against their own schema, so producers and consumers can be upgraded independently. Vehicle types are the worked example: `vehicleType` was added to `order.created` and `order.matched` as schema version 2 with a default of `CAR`, which is backward compatible. Matching claims a driver only if their status is AVAILABLE *and* their vehicle matches, in one atomic Redis script. A message that can't be decoded is a permanent error and goes straight to the dead-letter topic.

## 4c. Analytics

Rather than another hand-written consumer, analytics run in ksqlDB (see `ksql/statements.sql`): each Kafka topic is declared as a stream, and tables are derived with windowed aggregations and one stream-stream join (`order.created` with `order.matched` within a minute, to measure matching latency). Columns are inferred from the Avro schemas in the Schema Registry, which is why `schema-init` registers every schema before ksqlDB starts. The Ops dashboard reads the tables with pull queries. ksqlDB's own state is rebuilt from Kafka, so nothing here needs a database.

## 4d. Recovering after a reload

WebSocket pushes only reach a client that is connected at that moment, so both apps also recover by asking. The Driver App reads `GET /drivers/{id}/active-trip` (Trip Service). The Rider App reads `GET /riders/{id}/latest-order` (Order Service), and for a MATCHED order the trip from Trip Service; a still-PENDING order resumes "searching" and is polled every few seconds, which also covers a push missed while the socket reconnects. Composing the two calls in the client keeps the services independent (the trip doesn't know its rider, the order doesn't know its trip).

## 4e. Routing

Routing Service (port 3009) is HTTP only and stateless: no Kafka, no database, no Redis, no external API. `GET /route?fromLat&fromLng&toLat&toLng&vehicle=CAR|BIKE` (a token of any role) returns `{distanceKm, durationSec, polyline: [{lat, lng}...], vehicle, model: "detour-1.3"}`.

- **Model.** `distanceKm` = great-circle distance x 1.3 (a detour factor for a street grid). `durationSec` = distance / speed, where the speed depends on the vehicle and the hour of day in `Asia/Ho_Chi_Minh` (UTC+7, no daylight saving): slower at the peaks 07:00 to 09:00 and 17:00 to 19:00, faster at night. The polyline is the straight line between the two points in steps of about a kilometre (at most 20 segments). The same inputs at the same hour always give the same answer.
- **Limits.** Bad coordinates or an unknown vehicle give 400; a modelled distance above 200 km gives 422.
- **Callers.** Pricing Service asks for the distance behind a quote with a 300 ms timeout and **forwards the rider's own bearer token** (there is no service-to-service credential in this system, and Routing accepts any role); when Routing is off, slow or failing it uses the straight-line distance and counts it in `pricing_routing_fail_open_total{reason}`. The Rider App will call it for the distance and ETA (task 8.4).
- **Order Service does not call Routing**, so it cannot work out a routed distance itself. A booking whose quote is expired, reused or unchecked, and the replacement quote of a 409, therefore keep the **base fare carried in the quote's signed body** (which Pricing computed from the routed distance) and change only the multiplier. Only a booking with **no quote at all** has no routed distance to keep and is priced on the straight line, so that price can differ from a routed quote the rider never asked for ([production-design §17](production-design.md)).

## 4f. Promotions

Promotion Service (port 3010; Postgres and the outbox, no Redis) keeps `promotions(code, kind PERCENT|FIXED, value, max_discount, min_fare, vehicle_type, valid_from, valid_to, usage_limit, per_rider_limit, disabled_at)` and `redemptions(order_id, code, rider_id, discount, status, ...)`, unique per `(code, order_id)`. Amounts are whole VND.

- **Validate, do not call, at booking.** `GET /promotions/validate?code&price&vehicleType` (rider token) returns `{code, discount, price, finalPrice, currency, promoToken, expiresAt}`, or a 404/422 whose body is `{error, code}` with `code` one of `not_found`, `not_started`, `expired`, `min_fare`, `wrong_vehicle`, `limit_reached`, `already_used`. The discount is `price x value / 100` rounded down and capped at `max_discount` (PERCENT) or `value` (FIXED), never more than the price, so the final price is never negative. A disabled code is `not_found`. The `promoToken` is `base64url(JSON) "." base64url(HMAC-SHA256)` like a quote id (`services/pkg/promo`), valid 10 minutes, bound to rider, code, price, vehicle type and discount, signed with `PROMO_SECRET` (a key of its own, shared by Promotion Service and Order Service; the public development default is refused outside `APP_ENV=development`).
- **Booking.** `POST /orders` takes an optional `promoToken`. Order Service verifies it locally against the price it has just settled (it never calls Promotion Service): forged, expired, another rider's, for another price or for another vehicle type is `400 {error, code}` with `code` `promo_invalid`, `promo_expired`, `promo_wrong_rider`, `promo_wrong_price` or `promo_wrong_vehicle`. A token buys one booking (Redis `SET NX`, like a quote): a second use is `409 promo_used`; if Redis cannot be reached the discount is still applied (counted in `orders_promo_total{result="unchecked"}`). The order stores `promo_code` and `discount` (migration 00010); the response and `GET /orders/{id}` carry `promoCode`, `discount` and `finalPrice`; `order.created` v4 carries `promoCode` and `discount`.
- **Redemption.** Promotion Service consumes `order.created` (group `promotion-service-group`) and, for an order with a code, saves the redemption, the eventId dedupe row and the `promotion.redeemed` outbox row in one transaction, under the promotion's row lock so the counts are exact. A limit already reached is recorded as `OVER_LIMIT`, never a failure: the order exists with its discount. Three events release the redemption (`RELEASED`, no longer counted): `order.cancelled` (a rider cancelled a `PENDING` order), `trip.status.changed` with status `CANCELLED` (the trip was cancelled after the match, by the rider or the driver) and `order.no_driver_found`; other trip statuses are ignored. A release seen before the order leaves a marker so the redemption is recorded as released. No `promotion.redeemed` is published for a release.
- **Limits are soft.** The per-rider and global limits are checked at validation and again at redemption, but two orders in flight can both pass validation before either is recorded, so a limit can be exceeded by the number of orders in flight. Those are the `OVER_LIMIT` rows and the `PromotionRedeemedOverLimit` alert.
- **Payment.** Payment Service keeps `discount` next to the price in its local copy of the order and charges `price - discount`; the commission is on that net amount and `payment.recorded` carries the net amounts. A free ride (net 0) is skipped (`payments_skipped_total{reason="zero_amount"}`).
- **Admin.** `POST /promotions`, `GET /promotions`, `POST /promotions/{code}/disable` need an `admin` token. Disabling stops new validations only.
- **Frontend.** `validatePromotion()` and the admin helpers are in `packages/client`; the Rider App promo field and the Ops dashboard screen are task 9.4.

## 4g. Chat

Chat Service (port 3011; Postgres and the outbox, no Redis) lets the rider and the driver of a trip exchange short messages while they meet.

- **Who may talk.** `chat_trips(trip_id, rider_id, driver_id, status, updated_at)` is fed by `trip.status.changed` (group `chat-service-group`), deduped by `eventId` in the same transaction. The status only moves forward (`ASSIGNED`, `IN_PROGRESS`, then `COMPLETED` or `CANCELLED`) and an end never changes, so events delivered out of order, or twice, cannot reopen a finished chat. The identity of a caller is the token: a rider token whose subject is the trip's `riderId`, or a driver token whose subject is its `driverId`. Anyone else is 403 (`not_participant`), an unknown trip 404 (`unknown_trip`); an admin cannot read chats.
- **Sending.** `POST /trips/{id}/messages` with `{"text"}`: trimmed, 1 to 500 characters (counted as characters, not bytes), no control characters (a newline included), else 400 `invalid_text`. The reply is 201 with `{id, tripId, senderId, senderRole, text, createdAt}`. The message row and the `chat.message.sent` outbox row (key `tripId`) are written in one transaction.
- **Open and closed.** A chat is open while the trip is on. After `COMPLETED` or `CANCELLED` it stays writable for `CHAT_GRACE_MINUTES` (default 10), then sending is 409 `chat_closed`. History stays readable until the retention job deletes it.
- **Rate limit.** Per sender (role and id), sustained `CHAT_RATE_PER_MINUTE` (default 20) with a burst of a quarter of that, 429 `rate_limited` with `Retry-After` (also `retryAfter` in the body). It is in memory per instance, so with N instances a sender's real allowance is up to N times that.
- **Retries.** The optional `Idempotency-Key` header (1 to 128 printable ASCII characters) is kept with the message, scoped to the sender and trip: the same key and text returns the saved message (200, `Idempotent-Replayed: true`) and publishes nothing; the same key with other text is 422 `key_mismatch`. A replay is answered even after the chat has closed.
- **History.** `GET /trips/{id}/messages?before=&limit=` (limit 1 to 100, default 50) returns `{messages, nextBefore, tripStatus, open, closesAt}`, newest first, ordered by `(createdAt, id)`. `before` is the previous page's `nextBefore` (microseconds and the message id), so a page neither skips nor repeats a message when new ones arrive.
- **Push.** Notification Service consumes `chat.message.sent` and sends `{"type":"chat.message.sent","payload":{...}}` to the **recipient** only, on the rider or driver channel named by `recipientRole`, through the existing Redis Pub/Sub path. It is best-effort: the history above is how an app recovers what a push missed.
- **Privacy.** The text is never written to a log (ids and lengths only), by Chat Service or Notification Service. Messages and the rows of trips that ended are deleted after `CHAT_RETENTION_DAYS` (default 30). See [production-design §19](production-design.md).
- **Frontend.** `sendChatMessage()`, `listChatMessages()` and the types are in `packages/client`; the `ChatSheet` in the apps is task 10.3.

## 4h. Profiles and driver onboarding

Profile Service (port 3012; Postgres and the outbox, no Redis, consumes nothing) says who people are and who may drive.

- **Identity.** The id of a profile is the token's subject: `GET/PUT /riders/{id}` need a rider token and `GET/PUT /drivers/{id}` and `POST /drivers/{id}/application` a driver token, and the path must name the caller (403 `forbidden` otherwise, whether or not that profile exists). A body can never name another person or set the status. A rider or driver who has no profile reads an empty one (200; status `NONE` for a driver).
- **Fields.** `name` (required, 1 to 80 characters, with a letter), `phone` (optional; digits with spaces, dots, dashes, brackets, a leading `+`; 7 to 15 digits; loosely checked and not verified), and for drivers `vehicleType` (`CAR` or `BIKE`), `plate` (stored in capitals; letters of any script, digits, spaces, `-`, `.`; 3 to 16 characters; Vietnamese plates such as `51A-123.45` and `59-X1 123.45` pass, so do other countries'), `model` (50) and `color` (30). Text is trimmed, runs of white space become one space, control characters, line separators and bidirectional overrides are refused; characters are counted as characters. A 400 `invalid_profile` names the `field`.
- **Applying.** `POST /drivers/{id}/application` submits the saved profile: `NONE` or `REJECTED` becomes `PENDING` (201; the old reason is cleared); `PENDING` or `APPROVED` is returned unchanged (200, so a retry is safe); `SUSPENDED` is 409 `suspended`; a missing name, vehicle type or plate is 422 `incomplete_profile`. No event is published: `PENDING` is not a result.
- **Review (admin).** `GET /driver-applications?status=PENDING&limit&cursor` (oldest first, cursor stable while others are decided; includes the phone) and `POST /drivers/{id}/approve|reject|suspend` with `{"reason"}` (required for reject and suspend, 1 to 500 characters, shown to the driver). Allowed: `PENDING` to `APPROVED` or `REJECTED`, `APPROVED` to `SUSPENDED`, `SUSPENDED` to `APPROVED`; `REJECTED` to `PENDING` is the driver's new application. Anything else, a repeat included, is 409 `invalid_transition` with the status the driver has. Two admins acting at once get one winner (a row lock).
- **Vehicle changes.** A model or colour change is only a description. Changing the **plate or vehicle type of an APPROVED driver** puts them back to `PENDING` in the same transaction that queues a `driver.application.reviewed` with status `SUSPENDED`, `reviewedBy` `system`: Matching stops offering them trips (the vehicle nobody approved is never matched), the public view disappears, and an admin approving the new vehicle brings them back. Editing a `PENDING` application's plate changes nothing else; editing a `REJECTED` or `SUSPENDED` profile never changes the status (a driver cannot edit their way back).
- **`driver.application.reviewed`.** Key `driverId`; Avro `eventId, occurredAt, driverId, status, vehicleType, reason, reviewedBy` (`reason` and `reviewedBy` default to `""`). Written through the outbox with the status change. `occurredAt` is the decision time, forced to be later than the driver's previous result, so a consumer can order results by it alone.
- **Matching.** Matching Service consumes it in its own group `matching-service-driver-review-group` (retry tiers, DLQ) and calls `driverstatus.ApplyReview`, one Redis script per result: results not newer than `driver:{id}:review_at` are ignored (redelivery, out of order); `APPROVED` deletes `driver:{id}:suspended`, records the vehicle and, only if the driver has no status, is `OFFLINE`, `AVAILABLE`, or `ASSIGNED` with an expired lease, sets `AVAILABLE` (an `ASSIGNED` or `ON_TRIP` driver is never touched); `REJECTED` and `SUSPENDED` set the `suspended` key and, if the driver is free, `OFFLINE`. A driver who is on an order or a trip finishes it: the claim scripts (`Claim`, `ClaimForOrder`, `ClaimFirstForOrder`) refuse a suspended driver, and `Set` and `Transition` to `AVAILABLE` (the release after a trip, a cancelled match) record `OFFLINE` for one. Claim and suspension are both single scripts, so a concurrent claim either wins (the driver keeps that order) or is refused. Nothing in Matching requires a profile: a driver seeded straight into Redis, as the e2e and load tests do, is matched as before.
- **Public view.** `GET /drivers/{id}/public` (any signed-in user) returns `{driverId, name, vehicleType, plate, model, color}` only for an `APPROVED` driver, never the phone or the status; an unknown, unapproved or suspended driver is the same 404, so it does not tell who applied. Limited per caller (`PUBLIC_RATE_PER_MINUTE`, default 120).
- **Development data.** `profile-service seed-dev` (only with `APP_ENV=development`; `scripts/dev.sh` runs it) adds approved profiles for `driver-1..3` (CAR) and `driver-4..5` (BIKE) and profiles for `rider-1..3`, once (existing rows are left alone). It publishes **no** event: `matching-service seed-drivers` already made the drivers eligible, and an event published later could put a driver back to `AVAILABLE` after a demo script set them otherwise.
- **Privacy.** Names, phones and plates are never written to a log (ids only; a test fails if they appear) and responses carry `Cache-Control: no-store`. See [production-design §20](production-design.md).
- **Frontend.** `getRiderProfile()`, `updateRiderProfile()`, `getDriverProfile()`, `updateDriverProfile()`, `applyAsDriver()`, `getDriverPublic()`, `listDriverApplications()` and `reviewDriver()` are in `packages/client` (through the `/profile-api` dev proxy to port 3012); the screens are task 11.3.

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
