# Requirements — Ride-booking Mini

## Project goal
Build a mini ride-booking app (Grab-like) to learn:
- Kafka: producer/consumer, partitioning, consumer groups, event ordering, idempotency
- Design system: a shared component library used across multiple apps (rider, driver)

Non-goals: production-grade scaling, real payments, enterprise-level security.

## Actors
- **Rider** — requests a ride
- **Driver** — accepts and completes trips
- (No Admin actor in the initial scope)

## User Stories & Acceptance Criteria (EARS format)

### 1. Booking a ride
- **US-1.1**: As a rider, I want to create a ride order with pickup/dropoff points, so the system can find me a driver.
  - WHEN a rider submits a create-order request with valid pickup + dropoff, THE Order Service SHALL create the order with status `PENDING` and publish an `order.created` event.
  - IF pickup or dropoff is missing/invalid, THE Order Service SHALL reject the request with a 400 error and SHALL NOT publish the event.

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

## Technical constraints
- Kafka runs locally via Docker Compose, with Kafka UI for observing topics/offsets.
- No two-phase distributed transactions — eventual consistency is acceptable.
- The design system must be shared by at least 2 apps (rider app, driver app) — no per-app one-off components.

## Out of scope
- Real payments, payment gateway integration
- Detailed ratings, promotions, surge pricing
- Multi-region, high-availability Kafka cluster
- Full authentication (mock user/driver ids are acceptable initially)
