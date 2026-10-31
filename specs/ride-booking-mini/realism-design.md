# Design: making grab-mini behave like a real app (phases 8 to 11)

Status: in progress. Four services are added, each with a backend, infrastructure and a screen in the apps. The order below is the order of work: later phases reuse the `admin` role added in phase 8.

Every new service follows the pattern of Payment Service (see commits `90091f5`, `042cb7c`, `8cfdd25`): its own Go module in `go.work`, Avro schemas in `services/pkg/events/schemas`, topics, ACLs, Kubernetes manifests (via `deploy/k8s/generate.py`), KEDA scaling where it consumes, Prometheus alerts with tests, a runbook section, `scripts/dev.sh`, `Makefile`, and tests against real Postgres, Redis and Kafka that skip themselves when those are not set. A producer writes through the outbox; a consumer dedupes on `eventId` in the same transaction as its effect.

## Phase 8: Routing (ETA) and an `admin` role

**Why.** Distance today is a straight line, and no screen says when the driver arrives.

- **Auth.** Add the role `admin` (`auth.Admin`). The development token issuer may sign it; production issuers do not. Needed by phases 9 and 11 and used by the Ops dashboard.
- **`routing-service`** (HTTP only, stateless, no Kafka, no database). `GET /route?fromLat&fromLng&toLat&toLng&vehicle=CAR|BIKE` returns `distanceKm`, `durationSec` and a simple `polyline`. The model is deterministic and has no external API: great-circle distance times a detour factor (1.3), and a speed by vehicle type and hour of day (slower at peak). Callers: Rider App and Pricing.
- **Pricing** asks Routing for the distance with a short timeout and **fails open** to its own straight-line distance (counted in a metric), as it already does for the multiplier.
- **Rider App.** Before booking: distance and "about N min". After a match: "driver arrives in N min", recomputed from the driver's pushed location (throttled).
- Out of scope: real maps data, live traffic.

## Phase 9: Promotions

**Why.** Vouchers are a standard part of a ride app and they touch pricing and payment.

- **`promotion-service`** (Postgres, outbox). Tables: `promotions(code, kind PERCENT|FIXED, value, max_discount, min_fare, vehicle_type nullable, valid_from, valid_to, usage_limit nullable, per_rider_limit)` and `redemptions(order_id, code, rider_id, discount, status, ...)`.
- **Validate, do not call, at booking.** `GET /promotions/validate?code&price&vehicleType` (rider token) returns the discount and a **signed `promoToken`** (same HMAC scheme as price quotes; short-lived; bound to rider, code, price and vehicle). Order Service verifies the token locally and **locks `discount` onto the order**; it never calls Promotion Service on the booking path.
- **`order.created` v4** adds defaulted `promoCode` (empty) and `discount` (0). BACKWARD compatible. Order Service migration for both columns.
- **Redemption.** Promotion Service consumes `order.created` and records the redemption (unique per `(code, order_id)`, deduped by `eventId`), and consumes `order.cancelled` to release it. It publishes `promotion.redeemed` (key `orderId`) for analytics.
- **Limits.** The per-rider limit is enforced by the redemption transaction. The global `usage_limit` is checked at validation and again at redemption, but two orders in flight can both pass validation, so the global limit **can be exceeded by the number of in-flight orders**. This is documented, not hidden.
- **Payment.** Payment Service charges `price - discount`, commission on the net amount, and records the discount in its local copy of the order (migration). `payment.recorded` amounts stay the net amounts charged.
- **Ops dashboard (admin).** List, create and disable promotions (`POST /promotions`, admin token).
- **Rider App.** A promo-code field next to the price, showing the discount and the new total.
- Out of scope: per-user targeting, stacking codes, referral credits.

## Phase 10: Chat between rider and driver

**Why.** Pickup coordination is a core real-world need and shows a second realtime path.

- **`chat-service`** (Postgres, outbox). It learns who may talk from `trip.status.changed` (trip id, rider, driver, status). `POST /trips/{id}/messages` (text up to 500 characters, rider or driver of that trip only, rate limited per sender), `GET /trips/{id}/messages?before=&limit=` for history. A chat is open while the trip is active and read-only after it is `COMPLETED` or `CANCELLED`.
- **`chat.message.sent`** (key `tripId`) carries the message and both participant ids. Notification Service consumes it and pushes to the **other** participant over the existing WebSocket and Redis pub/sub path (ADR 0008): best-effort push, REST history is the recovery path.
- **Apps.** A `ChatSheet` component in `packages/ui` (built on `BottomSheet`), used by both apps; messages load by REST on open and append from pushes.
- Out of scope: read receipts, typing indicators, attachments, moderation beyond length and rate limit.

## Phase 11: Profiles and driver onboarding

**Why.** Everyone is currently a bare id. Real apps show who the driver is and decide who may drive.

- **`profile-service`** (Postgres, outbox). Riders: `GET/PUT /riders/{id}` (name, phone). Drivers: `GET/PUT /drivers/{id}` (name, vehicle type, plate, model, colour) and `POST /drivers/{id}/application` (status `PENDING`). A **public view** of a driver (name, vehicle, plate) is readable by any authenticated user. Admins review: `POST /drivers/{id}/approve|reject|suspend` with a reason.
- **`driver.application.reviewed`** (key `driverId`; status `APPROVED|REJECTED|SUSPENDED`, vehicle type). Matching Service consumes it: `APPROVED` makes the driver eligible (the same state seeding sets: `AVAILABLE` with the vehicle type), `REJECTED` and `SUSPENDED` take them out. The five seeded dev drivers get approved profiles in a seed migration so existing flows keep working.
- **Apps.** Rider App: profile screen. Driver App: an onboarding form and a status screen (pending, approved, rejected with reason); a driver who is not approved sees no trips. The matched card in the Rider App shows the driver's name, vehicle and plate. Ops dashboard: a review queue for applications (admin).
- Out of scope: document upload, background checks, real identity.

## Cross-cutting

- New topics: `promotion.redeemed`, `chat.message.sent`, `driver.application.reviewed` (partition and retention choices follow the neighbouring topics; chat has short retention).
- Documentation per service: `design.md` (service and topic tables), `requirements.md` (user stories US-7 onward, EARS), `production-design.md` (a numbered section each, with limitations), `runbook.md`, the guide's service and API tables, `deploy/README.md`. Cross-service ADRs and the README, diagram and Vietnamese summary are updated at the end.
- Honest limits stay: single region, sandbox payments, no real-cluster run.
