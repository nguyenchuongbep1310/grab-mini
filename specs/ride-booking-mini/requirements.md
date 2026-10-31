# Requirements — Ride-booking Mini

## Project goal
Build a mini ride-booking app (Grab-like) to learn:
- Kafka: producer/consumer, partitioning, consumer groups, event ordering, idempotency
- Design system: a shared component library used across multiple apps (rider, driver)

Non-goals: production-grade scaling, a real payment processor, enterprise-level security.

## Actors
- **Rider** — requests a ride
- **Driver** — accepts and completes trips
- **Admin** — an operator (the Ops dashboard; admin-only routes arrive with promotions and driver review). Only the development token issuer signs an `admin` token

## User Stories & Acceptance Criteria (EARS format)

### 1. Booking a ride
- **US-1.1**: As a rider, I want to create a ride order with pickup/dropoff points, so the system can find me a driver.
  - WHEN a rider submits a create-order request with valid pickup + dropoff, THE Order Service SHALL create the order with status `PENDING` and publish an `order.created` event.
  - IF pickup or dropoff is missing/invalid, THE Order Service SHALL reject the request with a 400 error and SHALL NOT publish the event.

- **US-1.2**: As a rider, I want to see the price before I book, including any surge, so I am never charged more than I agreed to.
  - WHEN a rider asks Pricing Service for a quote (pickup, dropoff, vehicle type), THE Pricing Service SHALL return a base fare, the surge multiplier of the pickup cell (1.0 to a configured maximum, rounded to 0.1), the price, and a signed `quoteId` valid for about 60 seconds.
  - WHEN a rider books with a valid `quoteId`, THE Order Service SHALL lock the quoted price on the order and publish it in `order.created` (`surgeMultiplier`, `price`).
  - WHEN a rider books without a quote, or with an expired one, THE Order Service SHALL read the current multiplier and lock the price at booking; IF an expired quote's multiplier has moved by more than a threshold, THE Order Service SHALL answer 409 with a new quote and SHALL NOT create the order.
  - IF a quote is forged, damaged, or for another rider or trip, THE Order Service SHALL reject the booking with a 400.
  - IF the multiplier cannot be read (Redis down, no key), THE system SHALL price at 1.0: pricing SHALL NOT block a booking.

### 2. Matching
- **US-2.1**: As the system, when a new order arrives, I want to find the nearest available driver to match.
  - WHEN the Matching Service consumes `order.created`, THE Matching Service SHALL find an `AVAILABLE` driver nearest to the pickup point within a configurable radius.
  - IF a driver is found, THE Matching Service SHALL publish `order.matched` (orderId, driverId) and set the driver's status to `ASSIGNED`.
  - IF no driver is found within N seconds, THE Matching Service SHALL publish `order.no_driver_found`.

### 3. Driver location updates
- **US-3.1**: As a driver, I want my app to send my location periodically so the rider can see me moving.
  - WHILE the driver app is open and the driver's status is `ASSIGNED` or `ON_TRIP`, THE Driver App SHALL publish `driver.location.updated` (driverId, lat, lng, timestamp) every 3-5 seconds.
  - THE Driver Location Service SHALL always keep the latest location by timestamp, discarding out-of-order (late-arriving) events.

### 4. Trip lifecycle
- **US-4.1**: As a driver, I want to confirm pickup to start the trip.
  - WHEN the driver confirms pickup, THE Trip Service SHALL move the trip to `IN_PROGRESS` and publish `trip.status.changed`.
- **US-4.2**: As a driver, I want to end the trip when arriving at the destination.
  - WHEN the driver confirms completion, THE Trip Service SHALL move the trip to `COMPLETED`, publish `trip.status.changed`, and release the driver back to `AVAILABLE`.
- **US-4.3**: As a rider or driver, I want to cancel a trip before it completes.
  - WHEN either party cancels a trip in a valid state (not yet `COMPLETED`), THE Trip Service SHALL move the trip to `CANCELLED` and publish `trip.status.changed`.
  - IF the trip is already `COMPLETED`, THE Trip Service SHALL reject the cancellation.
  - THE Trip Service SHALL record who cancelled (`RIDER` or `DRIVER`, taken from the caller's role) and an optional reason, and include both in `trip.status.changed`.
- **US-4.4**: As a rider, I want to cancel an order while still searching for a driver.
  - WHEN the owning rider cancels a `PENDING` order, THE Order Service SHALL move it to `CANCELLED` and publish `order.cancelled` in the same transaction; a repeated cancel SHALL succeed without a second event.
  - IF the order is already `MATCHED`, THE Order Service SHALL answer `409` telling the client to cancel the trip instead; any other caller gets `404`.
  - WHEN a trip is cancelled, THE Order Service SHALL move its `MATCHED` order to `CANCELLED`.
  - WHEN `order.cancelled` is published, THE Matching Service SHALL not claim a driver for that order (and SHALL release one it just claimed), THE Trip Service SHALL cancel any trip already created for it (and SHALL not create one from a late `order.matched`), and THE Notification Service SHALL tell the rider (and the matched driver, if any).

### 5. Realtime notifications
- **US-5.1**: As a rider, I want to see order status and driver location update in realtime without refreshing.
  - WHEN the Notification Service consumes an event related to the order/trip/driver location the rider is currently viewing, THE Notification Service SHALL push the update to the correct client over WebSocket within 1 second.

### 6. Idempotency & errors
- **US-6.1**: The system uses Kafka at-least-once delivery, so duplicate events may occur.
  - WHEN a consumer receives a duplicate event (same event id/order id + same target state) it has already processed, THE consumer SHALL skip it without creating a duplicate side effect.
- **US-6.2**: When a consumer fails to process an event and cannot retry successfully.
  - IF processing an event fails after N retries, THE consumer SHALL publish that event to the corresponding dead-letter topic instead of crashing the consumer.

### 7. Routing and the admin role
- **US-7.1**: As a rider, I want to know how far my trip is and how long it takes, so I can decide before I book.
  - WHEN a caller with a valid token of any role asks Routing Service for a route (from, to, vehicle `CAR` or `BIKE`), THE Routing Service SHALL return the distance in km, the duration in seconds, a polyline, the vehicle and the name of the model that produced them.
  - THE Routing Service SHALL be deterministic: THE distance SHALL be the great-circle distance times a fixed detour factor, AND THE duration SHALL depend only on the distance, the vehicle and the hour of day in Asia/Ho_Chi_Minh (slower at 07:00 to 09:00 and 17:00 to 19:00).
  - IF a coordinate is missing, not a finite number or out of range, or the vehicle is not `CAR` or `BIKE`, THE Routing Service SHALL answer 400.
  - IF the modelled distance exceeds 200 km, THE Routing Service SHALL answer 422.
  - IF the request has no valid token, THE Routing Service SHALL answer 401.
- **US-7.2**: As a rider, I want my fare to reflect the road distance, but never be blocked because Routing is down.
  - WHEN Pricing Service issues a quote, THE Pricing Service SHALL ask Routing Service for the distance with the rider's own token and a timeout of 300 ms, and compute the base fare on it.
  - IF Routing Service is disabled, times out, errors, answers an implausible distance, or refuses the trip as too long, THE Pricing Service SHALL compute the base fare on the straight-line distance, SHALL still issue the quote, and SHALL count it in `pricing_routing_fail_open_total`.
  - IF Routing Service times out or errors for ten minutes, THE system SHALL raise the ticket alert `PricingRoutingFailOpen`.
- **US-7.3**: As an operator, I want a distinct admin role, so later admin-only routes cannot be called by riders or drivers.
  - THE token issuer SHALL be able to sign a token with the role `admin`; a service SHALL treat a role other than `rider`, `driver` or `admin` as invalid.
  - WHILE `APP_ENV=production`, THE Auth Service (the development issuer) SHALL refuse to start unless `ALLOW_DEV_TOKENS=true`.
  - A route that names the roles it admits SHALL refuse an `admin` token unless `admin` is among them (403).
- **US-7.4** (Rider App screens, task 8.4, not built yet): As a rider, I want to see the distance and "about N min" before booking, and "driver arrives in N min" after a match.
  - WHEN the pickup, dropoff or vehicle changes, THE Rider App SHALL show the distance and the estimated time from Routing Service, and SHALL still allow booking if Routing is unavailable.
  - WHEN the rider has been matched, THE Rider App SHALL show the driver's estimated arrival, recomputed from the driver's pushed location at a throttled rate.
- **US-7.5**: As a rider, I want the price I pay to match the routed quote I saw, even if I book late.
  - WHEN a booking carries an expired quote, a quote that was already used, or one whose single-use check could not be made (Redis down), THE Order Service SHALL keep the base fare in that quote's signed body and apply only the current multiplier; the replacement quote of a 409 SHALL carry the same base fare.
  - WHEN a booking carries no quote, THE Order Service SHALL price it on the straight-line distance.

### 8. Promotions
- **US-8.1**: As a rider, I want to check a promo code against my trip before booking, and be told clearly why it does not apply.
  - WHEN a rider asks Promotion Service to validate a code for a price and vehicle type, THE Promotion Service SHALL return the discount, the final price and a promo token bound to that rider, code, price and vehicle type, valid for 10 minutes.
  - THE discount SHALL be a whole number of VND, never negative and never more than the price; a PERCENT discount SHALL be capped at the promotion's maximum.
  - IF the code does not exist or is disabled, THE Promotion Service SHALL answer 404 with the code `not_found`; IF it is not yet valid, expired, below the minimum fare, for another vehicle type, fully redeemed or already used by this rider up to the per-rider limit, it SHALL answer 422 with the code `not_started`, `expired`, `min_fare`, `wrong_vehicle`, `limit_reached` or `already_used`.
  - IF the caller is not a rider, THE Promotion Service SHALL answer 403; IF there is no valid token, 401.
- **US-8.2**: As a rider, I want the discount I was shown locked on my order.
  - WHEN a booking carries a promo token, THE Order Service SHALL verify it locally, without calling Promotion Service, against the rider, the settled price and the vehicle type, and SHALL store the code and the discount on the order and publish them in `order.created`.
  - IF the token is forged, expired, issued to another rider or for another price or vehicle type, THE Order Service SHALL answer 400 with a code (`promo_invalid`, `promo_expired`, `promo_wrong_rider`, `promo_wrong_price`, `promo_wrong_vehicle`) and SHALL not create the order.
  - WHEN a token has already been used for a booking, THE Order Service SHALL answer 409 `promo_used`.
  - WHEN a booking is retried with the same Idempotency-Key, THE Order Service SHALL replay the stored response and SHALL not use the token twice.
- **US-8.3**: As the business, I want redemptions recorded and limits enforced as far as the design allows.
  - WHEN Promotion Service consumes an `order.created` with a promo code, THE Promotion Service SHALL record one redemption per `(code, order)` and publish `promotion.redeemed`, in one transaction with the event's dedupe row; a redelivery SHALL change nothing.
  - IF a per-rider or global usage limit has already been reached when the order is recorded, THE Promotion Service SHALL record the redemption as `OVER_LIMIT` and SHALL not fail the order.
  - WHEN Promotion Service consumes `order.cancelled`, THE Promotion Service SHALL release the order's redemption, so the code can be used again.
  - The global limit MAY be exceeded by the number of orders in flight at the same time; this is documented, not hidden.
- **US-8.4**: As a rider, I want to pay the discounted price, and as a driver and the platform, I want commission on what was actually paid.
  - WHEN a trip completes, THE Payment Service SHALL charge the order's price less its discount, take the platform commission on that net amount, and publish `payment.recorded` with the net amounts.
  - IF the net amount is 0, THE Payment Service SHALL charge nothing and count the ride in `payments_skipped_total{reason="zero_amount"}`.
  - IF the discount is negative or more than the price, THE Payment Service SHALL dead-letter the order event rather than guess.
- **US-8.5**: As an operator, I want to create, list and disable promotions.
  - WHEN a caller with an `admin` token posts a valid promotion, THE Promotion Service SHALL create it (201); IF the code exists, 409; IF a field is invalid, 400 with the code `invalid_promotion`.
  - WHEN an admin disables a promotion, THE Promotion Service SHALL stop validating it; orders that already carry its discount SHALL keep it.
  - IF the caller is a rider or a driver, THE Promotion Service SHALL answer 403.
- **US-8.6** (Rider App and Ops dashboard screens, task 9.4, not built yet): As a rider, I want a promo-code field next to the price, showing the discount and the new total; as an operator, I want to list, create and disable promotions from the Ops dashboard.

## Technical constraints
- Kafka runs locally via Docker Compose, with Kafka UI for observing topics/offsets.
- No two-phase distributed transactions — eventual consistency is acceptable.
- The design system must be shared by at least 2 apps (rider app, driver app) — no per-app one-off components.

## Out of scope
- A real payment gateway or card processor (rides are settled through a double-entry ledger against a sandbox provider; no payouts, refunds or reconciliation)
- Per-user targeting of promotions, stacking several codes on one order, referral credits
- Multi-region, high-availability Kafka cluster
- Full authentication (mock user/driver ids are acceptable initially)
