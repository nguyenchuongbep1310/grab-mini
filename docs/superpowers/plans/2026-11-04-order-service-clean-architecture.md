# order-service Clean Architecture Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move `services/order-service` from a flat `package main` to `internal/{domain,app,adapters/*,integration}` with the dependency rule enforced by `pkg/archtest`, changing no observable behaviour.

**Architecture:** Third service in the ADR 0014 order (after `payment-service` and `trip-service`, both finished and in `main`). Pure rules go to `domain`; use cases and their ports go to `app`; Postgres, Redis, Kafka, HTTP and Prometheus become adapters; `main.go` stays the composition root. New packages are added first and nothing calls them until a single switch-over task deletes the flat code, so every commit compiles and passes its tests.

**Tech Stack:** Go 1.26 workspace (`go.work`), pgx/v5, go-redis/v9, kafka-go, Prometheus client, `grab-mini/pkg/{auth,outbox,eventcodec,events,kafkaconsumer,surge,promo,archtest,...}`.

**Spec:** `docs/adr/0014-clean-architecture-for-go-services.md` (layout, dependency rule, "What is explicitly not changed", migration recipe, rollback). Reference implementations to copy the shape from: `services/payment-service/internal/` and `services/trip-service/internal/`.

## Global Constraints

Copied from the ADR; every task's requirements include them.

- Dependency rule, enforced by `services/pkg/archtest` (`archtest.Default(module).Test(t, ".")` from `internal/arch_test.go`): `domain` imports the standard library only, without `net/http`, `database/sql`, `os` and the other I/O packages in `Rules.StdlibBanned`; `app` imports `domain`, the standard library (same exclusions) and `Rules.AppAllowed`; neither imports `services/pkg/*`, an I/O library or an adapter; an adapter may import anything except another adapter.
- Not changed: HTTP routes, status codes and JSON bodies (including each error response's `Content-Type` and exact body bytes); topics, keys, consumer groups and Avro schemas; tables, constraints and migration files `00001` to `00010` (none edited, renumbered or moved); environment variable names and defaults; metric names, labels and help text; log lines that docs or alerts reference; `deploy/`, `scripts/`, `docker-compose.yml`, `services/pkg/events`.
- `main.go` and `env.go` stay at the service root (`go build .` is what the Dockerfile and `deploy/verify.sh` run). `//go:embed migrations/*.sql` stays in `main` (the `migrate` subcommand keeps working).
- The consumer group stays a literal `GroupID: "order-service-group"` in a non-test `.go` file: `deploy/k8s/check.py` reads it by regex (it already searches recursively).
- Prometheus counters register at package init (`promauto`): exactly one package (`adapters/metrics`) declares each of the six existing counters.
- Tests: domain tests need no containers; the integration tests keep running against real Postgres, Redis and Kafka. Moved tests keep their names unless a task says otherwise; no test is dropped.
- One service, one pull request. Rollback is `git revert` of this service's commits; nothing in `deploy/` changes.
- Do not fix unrelated bugs: list them in the task report.

## Decisions this plan makes (the executor must not reopen them)

1. **`pkg/surge` and `pkg/promo` are on the app allow-list.** Both import only `crypto/*`, `encoding/*`, `math` and `strings` (their `SignerFromEnv` reads the environment but only `main` calls it). The quote and promo decision logic (`pricing.go`, `promo.go`) therefore moves into `app` with `*surge.Signer`, `surge.Quote` and `*promo.Signer` used directly. The order-service arch test sets `rules.AppAllowed = append(rules.AppAllowed, "grab-mini/pkg/surge", "grab-mini/pkg/promo")`. The shared `Default` is not changed.
2. **The stored idempotent response is rendered by the HTTP adapter, through a port.** The record saved with the order must hold the exact bytes the first attempt returned, and a replay of a row written by the old binary must be byte-identical. `app` builds a `Receipt` value and calls `ReceiptEncoder`; the HTTP adapter's encoder keeps the old `map[string]any` marshal (keys sorted, trailing newline).
3. **Transaction scope is preserved.** With an `Idempotency-Key`, one transaction opens first, takes the advisory lock, looks the key up, then prices, builds the event and inserts, all inside `UnitOfWork.Do`. Without a key, pricing and encoding run first and a transaction covers only the insert.
4. **Log message texts stay as they are** (they move into the use cases unchanged); error mapping to the HTTP bodies stays in the HTTP adapter.

## Review Focus

Inputs and conditions the ADR implies but the existing tests do not pin; each has a test in the owning task.

1. **Error response bytes.** The validation and not-found paths answer through `http.Error` (so `text/plain` with a JSON-looking body) in some places and through explicit JSON writers in others; a tidy-up would change the wire. Pinned by a golden table recorded from the old code in Task 1 and asserted again in Task 6.
2. **Replay of a row written before the migration.** A stored `idempotency_keys.response` must come back byte for byte. Pinned by a golden of the receipt encoding in Task 1, asserted in Task 6.
3. **Release on every failure path.** After a quote or promo token is claimed, any failure (promo refused, encode error, save error, duplicate key) must give both claims back, even when the request context is already cancelled. Pinned with fakes in Task 3 (pricing, promo) and Task 4 (the booking flow).
4. **Match versus cancel racing** and a **cancel of a trip whose order id is not a UUID**: the status-guarded `UPDATE`s must stay conditional and the second case stays a permanent error (no retry). Pinned in Task 4 and Task 5.
5. **Malformed ids** (`GET /orders/{id}`, `POST /orders/{id}/cancel` with a non-UUID id) answer 404, never 500. Pinned in Task 1 (golden) and Task 6.

---

### Task 1: Baseline and characterization tests

**Files:**
- Create: `services/order-service/golden_test.go` (still `package main`; moved in Task 7)
- Create (not committed): `<workspace>/baseline.md` in the SDD workspace

**Interfaces:**
- Produces: `golden_test.go` with `TestErrorResponsesAreStable` and `TestTheReceiptEncodingIsStable`; the baseline numbers every later task compares against.

- [ ] **Step 1: Record the baseline** in `baseline.md`: `gofmt -l`, `go vet`, `go build ./...` results; the passing-test count of `services/order-service` (`go test -race -count=1 -v ./...`) with real Postgres, Redis and Kafka; the e2e result (`services/e2e-test`, run twice, fresh Kafka each); the route table (method, pattern, roles, limiter) from `routes`; the metric names, labels and help strings; all `log.Printf` format strings; the env vars with defaults; the topics, keys and `GroupID`; `sha256` of every file in `migrations/`; `sha256` of `kubectl kustomize deploy/k8s/base`.
- [ ] **Step 2: Write `TestErrorResponsesAreStable`.** A table over the paths that need no database: `POST /orders` with no identity (401 via `auth.Deny`), with a body naming another rider (403), invalid JSON, a bad `Idempotency-Key` (empty, 129 characters, a control character, two headers), a missing pickup, latitude 91, an unknown vehicle type, lowercase `car`; `GET /orders/not-a-uuid` and `POST /orders/not-a-uuid/cancel` (404); a cancel reason of 201 runes (400); `GET /riders/{id}/latest-order` for another rider (403). Each row asserts status, `Content-Type` and the exact body string. Run it against the unchanged code and paste the observed values in as the expectation.
- [ ] **Step 3: Write `TestTheReceiptEncodingIsStable`** (needs `TEST_DATABASE_URL`, same gating as `main_test.go`): book once with no quote and no promo, assert the body equals `{"currency":"VND","discount":0,"finalPrice":<price>,"orderId":"<id>","price":<price>,"status":"PENDING","surgeMultiplier":1}` + `"\n"` with the actual id and price substituted, and book once with a promo token and assert `promoCode` appears as an extra sorted key.
- [ ] **Step 4: Run** `cd services/order-service && go test -count=1 -run 'TestErrorResponsesAreStable|TestTheReceiptEncodingIsStable' ./...` with `TEST_DATABASE_URL` and `TEST_REDIS_ADDR` set. Expected: PASS.
- [ ] **Step 5: Commit** `golden_test.go` only: `git add services/order-service/golden_test.go && git commit -m "order-service: pin error responses and the receipt encoding before the migration"`.

### Task 2: `internal/domain`

**Files:**
- Create: `services/order-service/internal/domain/{order.go,access.go,booking.go,idempotency.go,cancel.go}` and `*_test.go` for each.

**Interfaces:**
- Produces (package `domain`, standard library only):
  - `type Status string` with `StatusPending`, `StatusMatched`, `StatusNoDriver`, `StatusCancelled` (`"PENDING"`, `"MATCHED"`, `"NO_DRIVER"`, `"CANCELLED"`); `type VehicleType string` with `VehicleCar`, `VehicleBike` and `func (VehicleType) Valid() bool`; `type LatLng struct{ Lat, Lng float64 }`; `func ValidLatLng(*LatLng) bool` (non-nil, `|lat| <= 90`, `|lng| <= 180`).
  - `type Order struct{ ID, RiderID string; Pickup, Dropoff LatLng; VehicleType VehicleType; Status Status; CancelledAt *time.Time; CancelledBy, CancelReason string; SurgeMultiplier float64; Price *float64; PromoCode string; Discount int64; DriverID string }`, `func (Order) FinalPrice() *float64` (`max(price - discount, 0)`, nil when `Price` is nil) and `var ErrOrderNotFound`.
  - `type Role string` (`RoleRider`, `RoleDriver`), `type Actor struct{ Subject string; Role Role }`, `func (Order) MayBeReadBy(Actor) bool` (the rider that owns it; a driver only when `DriverID` is non-empty and equal to `Subject`).
  - `type BookingRequest struct{ Pickup, Dropoff *LatLng; VehicleType VehicleType; QuoteID, PromoToken string }`, `func (BookingRequest) Normalize() (BookingRequest, error)` returning `ErrBadCoordinates` (either point invalid) before `ErrBadVehicleType` (empty defaults to `VehicleCar`; invalid is refused).
  - `const IdempotencyTTL = 24 * time.Hour`, `func ParseIdempotencyKey(headerValues []string) (string, error)` (`ErrBadIdempotencyKey` for more than one value, an empty or over-128-character key, or any byte outside `0x20..0x7e`; no value means `"", nil`), `func (BookingRequest) Fingerprint() string` (the existing sha256 of the normalized JSON, unchanged), `type KeyRecord struct{ Key, Fingerprint, OrderID string; Status int; Body []byte }`.
  - `const MaxCancelReasonLength = 200`, `func NormalizeCancelReason(string) (string, error)` (trim, then `ErrReasonTooLong` above 200 runes), `const CancelledByRider = "RIDER"`.

- [ ] **Step 1: Write the failing tests**, moving the assertions of `TestIdempotencyKeyValidation`, `TestFingerprintIgnoresFormattingButNotMeaning`, `TestOnlyTheRiderAndTheMatchedDriverMayReadAnOrder` (names kept), and adding `TestFinalPriceNeverGoesBelowZeroAndIsNilWithoutAPrice`, `TestNormalizeChecksCoordinatesBeforeTheVehicleType`, `TestCancelReasonIsTrimmedThenCountedInRunes` (a 200-rune reason with 2 spaces each side passes; 201 runes fails; a multibyte rune counts once).
- [ ] **Step 2: Run** `go test ./internal/domain/...` in `services/order-service`. Expected: FAIL, undefined symbols.
- [ ] **Step 3: Implement the package** by moving the logic out of `idempotency.go` (`idempotencyKey`, `fingerprint`), `main.go` (`isValidLatLng`, the `FinalPrice` arithmetic in `queryOrder`, `mayReadOrder`) and `cancel.go` (trim and rune count). Keep the fingerprint's struct field order exactly: it is the hash input.
- [ ] **Step 4: Run** `go vet ./internal/domain/... && go test -race -count=1 ./internal/domain/...`. Expected: PASS. The flat code is untouched, so `go build ./...` still passes.
- [ ] **Step 5: Commit** `git add services/order-service/internal/domain && git commit -m "order-service: add internal/domain (new files only)"`.

### Task 3: `internal/app` pricing and promo

**Files:**
- Create: `services/order-service/internal/app/{ports.go,pricing.go,promo.go,errors.go}`, `fakes_test.go`, `pricing_test.go`, `promo_test.go`

**Interfaces:**
- Consumes: `domain.VehicleType`, `domain.LatLng`.
- Produces (package `app`; may import `grab-mini/pkg/surge`, `grab-mini/pkg/promo`, `github.com/google/uuid`):
  - Ports: `type MultiplierSource interface{ Multiplier(ctx, vehicle, cell string) float64 }` (fails open: an adapter answers 1.0 on error); `type ClaimStore interface{ Claim(ctx, id string, ttl time.Duration) (first bool, err error); Release(ctx, id string) }`; `type Metrics interface{ OrderCreated(vehicle string); OrderCancelled(); Pricing(result string); SurgeFailOpen(); Promo(result string); Idempotent(result string) }`.
  - `type Pricer struct{ Signer *surge.Signer; Mult MultiplierSource; Claims ClaimStore; Tolerance, CellDeg float64; Now func() time.Time; Metrics Metrics }`; `func (p *Pricer) Price(ctx, riderID string, req domain.BookingRequest) (Pricing, error)`; `type Pricing struct{ Multiplier, Price float64; ClaimedQuote string }`; `func (p *Pricer) Release(ctx, Pricing)`; `var ErrInvalidQuote`; `type QuoteConflict struct{ NewQuote surge.Quote; Reason string }` (`Error()` as today) and `const ReasonQuoteUsed`.
  - `type PromoChecker struct{ Signer *promo.Signer; Claims ClaimStore; Now func() time.Time; Metrics Metrics }`; `func (c *PromoChecker) Apply(ctx, riderID string, vehicle domain.VehicleType, price float64, token string) (Applied, error)`; `type Applied struct{ Code string; Discount int64; /* unexported claim id */ }`; `func (c *PromoChecker) Release(ctx, Applied)`; `type PromoError struct{ Code, Message string; Conflict bool }` (Conflict is the old 409; the HTTP adapter maps it).

- [ ] **Step 1: Write fakes and move the pricing and promo tests** that use no database or Redis (`TestAValidQuoteLocksItsPriceEvenIfTheMultiplierMoved`, `TestNoQuoteIsPricedAtTheCurrentMultiplier`, `TestAnExpiredQuoteWithinToleranceIsRepricedAtTheCurrentMultiplier`, `TestAnExpiredQuoteThatMovedTooFarGivesANewQuoteToConfirm`, `TestTamperedForgedAndForeignQuotesAreInvalid`, the base-fare tests, `TestWithRedisDownABookingIsRepricedNotRejected`, and the promo refusals, single-use and Redis-down tests) onto fake `MultiplierSource`, `ClaimStore` and `Metrics`. Names kept; they assert the metric labels through the fake `Metrics`.
- [ ] **Step 2: Add** `TestAClaimIsReleasedEvenWhenTheRequestContextIsCancelled` (Review Focus 3: cancel the context after `Price`, call `Release`, assert the fake saw the release) and `TestARefusedPromoLeavesTheQuoteClaimedForTheCallerToRelease` (documents who releases what).
- [ ] **Step 3: Run** `go test ./internal/app/...`. Expected: FAIL (undefined).
- [ ] **Step 4: Implement** by moving `pricer.Price`, `currentMultiplier`, `repriced`, `conflict`, `promoChecker.Apply`, `refusal` unchanged, replacing `promauto` counters with `Metrics` calls (same label strings), and `redisMultipliers`/`redisClaims` stay behind the ports (Task 5). `Release` keeps `context.WithoutCancel`-equivalent behaviour in the adapter, not here.
- [ ] **Step 5: Run** `go vet ./internal/app/... && go test -race -count=1 ./internal/app/...`. Expected: PASS.
- [ ] **Step 6: Commit** `git add services/order-service/internal/app && git commit -m "order-service: add internal/app pricing and promo (new files only)"`.

### Task 4: `internal/app` order use cases

**Files:**
- Create: `services/order-service/internal/app/{service.go,commands.go,booking.go,cancel.go,outcomes.go,queries.go}`; extend `ports.go`, `errors.go`, `fakes_test.go`; add `booking_test.go`, `cancel_test.go`, `outcomes_test.go`, `queries_test.go`.

**Interfaces:**
- Consumes: everything from Tasks 2 and 3.
- Produces:
  - Ports: `OrderReader{ ByID(ctx, id string) (domain.Order, error); LatestByRider(ctx, riderID string) (domain.Order, error) }` (both `domain.ErrOrderNotFound` when none); `KeyStore{ Lookup(ctx, riderID, key string) (*domain.KeyRecord, error); Purge(ctx) (int64, error) }` (`Lookup` returns nil, nil for none or expired and deletes an expired row, as `lookupKey` does); `Outcomes{ Set(ctx, orderID string, status domain.Status, driverID string) (changed bool, err error); CancelMatched(ctx, orderID, by, reason string) (changed bool, err error) }`; `UnitOfWork{ Do(ctx, fn func(Tx) error) error }`; `Tx{ LockKey(ctx, riderID, key string) error; Lookup(ctx, riderID, key string) (*domain.KeyRecord, error); SaveKey(ctx, riderID string, rec domain.KeyRecord) error; InsertOrder(ctx, NewOrder) error; CancelPending(ctx, orderID, reason string) (cancelled bool, err error); QueueOrderCreated(ctx, orderID string, payload []byte) error; QueueOrderCancelled(ctx, orderID string, payload []byte) error }`; `Encoder{ OrderCreated(ctx, OrderCreated) ([]byte, error); OrderCancelled(ctx, OrderCancelled) ([]byte, error) }`; `ReceiptEncoder func(Receipt) []byte`; `Waker interface{ Wake() }` (the outbox relay, woken only after a commit that queued an event).
  - Value types: `NewOrder{ ID, RiderID string; Pickup, Dropoff domain.LatLng; VehicleType domain.VehicleType; Multiplier float64; Price *float64; PromoCode string; Discount int64 }` (a zero `Multiplier` is stored as 1.0, as today); `OrderCreated{ OrderID, RiderID string; Pickup, Dropoff domain.LatLng; VehicleType domain.VehicleType; SurgeMultiplier float64; Price *float64; PromoCode string; Discount int64 }`; `OrderCancelled{ OrderID, RiderID, CancelledBy, Reason string }`; `Receipt{ OrderID string; SurgeMultiplier, Price, FinalPrice float64; Discount int64; PromoCode string }` (status is always `PENDING`; the currency is `surge.Currency`).
  - `var ErrDuplicateKey` (returned by `Tx.SaveKey` for the primary-key conflict), `ErrKeyMismatch`, `ErrForbidden`, `ErrInternal`, `type BadOutcomeError struct{ Reason string }` with `Permanent() bool { return true }` and the existing text `unusable matching outcome: …`.
  - `type Service struct` built by `func New(Deps) *Service` with `Deps{ Orders OrderReader; Keys KeyStore; Outcomes Outcomes; UoW UnitOfWork; Encoder Encoder; Receipt ReceiptEncoder; Pricer *Pricer; Promos *PromoChecker; Relay Waker; Metrics Metrics; Now func() time.Time }`.
  - `func (s *Service) CreateOrder(ctx, CreateOrderCommand) (CreateOrderResult, error)`; `CreateOrderCommand{ RiderID, IdempotencyKey string; Request domain.BookingRequest }`; `CreateOrderResult{ Status int; Body []byte; Replayed bool }` (201 and the receipt bytes for a new order; the stored status and bytes for a replay); errors `*QuoteConflict`, `ErrInvalidQuote`, `*PromoError`, `ErrKeyMismatch`, `ErrInternal`.
  - `func (s *Service) CancelOrder(ctx, actor domain.Actor, orderID, reason string) (CancelResult, error)` with `CancelResult{ Order domain.Order; Conflict bool; Message string }` (`domain.ErrOrderNotFound` for a malformed id, a missing order or another rider's; the two conflict messages are the existing strings).
  - `func (s *Service) GetOrder(ctx, actor domain.Actor, id string) (domain.Order, error)`, `func (s *Service) LatestOrder(ctx, actor domain.Actor, riderID string) (domain.Order, error)` (`ErrForbidden` when `actor.Subject != riderID`), `func (s *Service) SetMatched(ctx, orderID, driverID string) error`, `SetNoDriver(ctx, orderID string) error`, `CancelMatchedOrder(ctx, orderID, by, reason string) error`, `PurgeExpiredKeys(ctx) (int64, error)`.

- [ ] **Step 1: Write the failing tests with in-memory fakes of every port.** Booking: `TestAnOrderAndItsEventAreSavedTogether`, `TestNoOrderIsLeftBehindWhenTheEventCannotBeQueued`, `TestARetriedBookingReturnsTheSameOrderAndPublishesOnce`, `TestTheSameKeyWithAnotherBodyIs422` (as `ErrKeyMismatch`), `TestAKeyBelongsToItsRider`, `TestWithoutAKeyEveryRequestIsABooking`, `TestAReplayIsAnsweredBeforeTheQuoteIsLookedAt`, `TestADuplicateKeyAtSaveFallsBackToReplayingTheWinner`; release (Review Focus 3): `TestEveryFailureAfterTheClaimsGivesBothBack` (table: promo refused, encoder fails, `InsertOrder` fails, `SaveKey` fails with something other than `ErrDuplicateKey`). Flow order (Decision 3): `TestWithAKeyThePricingRunsInsideTheTransaction` and `TestWithoutAKeyThePricingRunsBeforeTheTransactionOpens` using a recording fake. Cancel: the existing cancel and race tests that need no database (`TestARiderCancelsAPendingOrder`, `TestCancellingAnOrderTwiceIsAnIdempotentSuccess`, `TestAMatchedOrderCannotBeCancelledHere`, `TestAnOrderWithNoDriverCannotBeCancelled`, `TestOnlyTheOrdersRiderMayCancelIt`) plus `TestACancelThatLosesTheRaceQueuesNoEvent`. Outcomes: `TestAnOutcomeForANonUUIDOrderIsPermanent`, `TestACancelledTripWithABadOrderIdIsPermanentlyUnusable`, `TestACancelledTripCancelsItsMatchedOrder` (fake). Queries: `TestOrderAccessIsLimitedToTheOwnerAndDrivers` (404 for anyone who may not read it, `ErrForbidden` on another rider's latest order).
- [ ] **Step 2: Run** `go test ./internal/app/...`. Expected: FAIL.
- [ ] **Step 3: Implement** by moving `handleCreateOrder` (everything after request decoding and header parsing), `insertOrder`/`insertOrderTx`, `handleCancelOrder` (after the id and reason checks), `cancelPending`, `outcomes.go` and the key purge into the use cases above, with the operations in the same order as today. Event and receipt construction: `OrderCreated` and `OrderCancelled` are plain app structs mirroring the fields of `events.OrderCreated` and `events.OrderCancelled` (Task 6's encoder maps them). `CreateOrder` calls `Relay.Wake()` and `Metrics.OrderCreated`/`Idempotent("new")` only after a successful commit.
- [ ] **Step 4: Run** `go vet ./internal/app/... && go test -race -count=1 ./internal/app/...`. Expected: PASS.
- [ ] **Step 5: Commit** `git add services/order-service/internal/app && git commit -m "order-service: add the order use cases to internal/app (new files only)"`.

### Task 5: Postgres, Redis and metrics adapters

**Files:**
- Create: `services/order-service/internal/adapters/postgres/{connect.go,store.go,uow.go}`, `internal/adapters/redis/{redis.go,contract_test.go}`, `internal/adapters/metrics/prometheus.go`

**Interfaces:**
- Consumes: the ports of Tasks 3 and 4.
- Produces:
  - `postgres.Connect(ctx, url string, migrations embed.FS) (*pgxpool.Pool, error)` and `postgres.Migrate(ctx, url string, migrations embed.FS) error` (the old `connectDB` and `runMigrate` bodies, with the same log text); `postgres.NewStore(*pgxpool.Pool) *Store` implementing `OrderReader`, `KeyStore` and `Outcomes` with the existing SQL verbatim (`orderColumns`, `queryOrder`, the three guarded `UPDATE`s, `lookupKey`, `purgeExpiredKeys` with its 5000-row batch); `postgres.NewUnitOfWork(*pgxpool.Pool, source string) *UnitOfWork` implementing `UnitOfWork` and `Tx` (the `pg_advisory_xact_lock` statement, `saveKey`, `insertOrderTx` SQL, the conditional `CancelPending`, `outbox.Add` for the two events; `SaveKey` maps the `idempotency_keys_pkey` violation to `app.ErrDuplicateKey`).
  - `redis.NewMultipliers(rdb *redis.Client, m app.Metrics) app.MultiplierSource` (300 ms read timeout, fails open to 1.0 with the same log line and `SurgeFailOpen()`), `redis.NewClaims(rdb) app.ClaimStore` (`quote:used:<sha256 hex>` key, `SET NX` with the TTL, delete on release through `context.WithoutCancel`).
  - `metrics.New() *Metrics` implementing `app.Metrics`; owns the six counters with their current names, labels and help text.

- [ ] **Step 1: Write the failing tests.** Postgres (gated on `TEST_DATABASE_URL`, using `internal/integration`'s helper once it exists; until then a local `testDB(t)` in the package): `TestTheGuardedUpdatesNeverOverwriteACancelledOrder` (a `MATCHED` outcome after `CANCELLED` changes nothing; `CancelMatched` only from `MATCHED`), `TestAnExpiredKeyIsDeletedOnLookup`, `TestTheUniqueKeyStopsASecondOrderEvenWithoutTheLock`, `TestAnOrderAndItsOutboxRowCommitTogether`, `TestPriceAndMultiplierArePersistedAndOldOrdersReadAsUnpriced`. Redis (gated on `TEST_REDIS_ADDR`): `TestQuoteMarksUseSetNXWithATTL`, `TestRedisMultipliersReadsAndFailsOpen`, and a contract test that the claim key format equals the old one. Metrics: `TestTheCountersKeepTheirNamesLabelsAndHelp` comparing against the strings recorded in Task 1.
- [ ] **Step 2: Run** `go test ./internal/adapters/...`. Expected: FAIL.
- [ ] **Step 3: Implement** by moving the code named above; no SQL text changes.
- [ ] **Step 4: Run** `go vet ./... && go test -race -count=1 ./internal/adapters/...` with Postgres and Redis. Expected: PASS; `go build ./...` of the whole module still passes (the flat code is untouched).
- [ ] **Step 5: Commit** `git add services/order-service/internal/adapters && git commit -m "order-service: add the postgres, redis and metrics adapters (new files only)"`.

### Task 6: Kafka and HTTP adapters

**Files:**
- Create: `services/order-service/internal/adapters/kafka/{handler.go,encoder.go,contract_test.go}`, `internal/adapters/http/{api.go,api_test.go}`

**Interfaces:**
- Produces:
  - `kafka.NewEncoder(codec *eventcodec.Codec) *Encoder` implementing `app.Encoder` (stamps `EventID` with a new UUID and `OccurredAt` with `time.Now().UTC().Format(time.RFC3339Nano)`: Matching measures wait time from it); `kafka.NewHandler(svc *app.Service, codec *eventcodec.Codec) *Handler` with `Handle(ctx, kafka.Message) error` for the three consumed topics (`TripStatusChanged` other than `CANCELLED` is ignored; an unexpected topic is an error) and `Run(ctx, brokers []string)` containing the literal `GroupID: "order-service-group"`, the same three topics, `LogLabel: "[order-service]"` and `RetryTiers: kafkaconsumer.DefaultRetryTiers`; a contract test pinning the domain and app copies of the wire constants (statuses, `CancelledByRider`) to `pkg/events`.
  - `http.NewAPI(svc *app.Service, verifier *auth.Verifier) *API` with `Routes() *http.ServeMux` registering exactly the old patterns, roles and the `auth.LimiterFromEnv(1, 10)` limiter on `POST /orders` and `POST /orders/{id}/cancel`; `http.ReceiptEncoder() app.ReceiptEncoder` (Decision 2).

- [ ] **Step 1: Write the failing tests.** Kafka: `TestOutcomeMessagesReachTheUseCases` (per topic, with a fake service port or the real `app.Service` over fakes) and the contract test. HTTP, against the golden recorded in Task 1: `TestErrorResponsesAreStable` and `TestTheReceiptEncodingIsStable` re-expressed over `NewAPI` with a fake-backed `app.Service` (same rows, same expected strings), plus `TestEveryAppErrorMapsToItsOldResponse` (table over `*QuoteConflict` to 409 with `error` and `newQuote`, `ErrInvalidQuote` to 400, `*PromoError` to its code with 409 or 400, `ErrKeyMismatch` to 422 with the `Idempotent-Replayed` header absent, a replay to the stored status with `Idempotent-Replayed: true`, `ErrInternal` to 500 `{"error":"internal error"}`, `ErrForbidden` through `auth.Deny` 403) and `TestCreateOrderTakesTheRiderFromTheToken`.
- [ ] **Step 2: Run** `go test ./internal/adapters/kafka/... ./internal/adapters/http/...`. Expected: FAIL.
- [ ] **Step 3: Implement.** The HTTP adapter keeps what is transport-only: decoding, `riderId` versus token (403 via `auth.Deny`), `ParseIdempotencyKey` over `r.Header.Values("Idempotency-Key")`, the 8 KiB body limit on cancel, path ids, and every response writer with its exact `Content-Type` and body bytes; it also converts `domain.Order` to the JSON view (same tags, `omitempty`, `finalPrice` from `FinalPrice()`, `driverId` never serialized). Everything else is a call into `app.Service`.
- [ ] **Step 3b: Check** the arch rule by eye for these two packages: they may import `pkg/*` and the app, and not each other or other adapters.
- [ ] **Step 4: Run** `go vet ./... && go test -race -count=1 ./internal/adapters/...`. Expected: PASS.
- [ ] **Step 5: Commit** `git add services/order-service/internal/adapters && git commit -m "order-service: add the kafka and http adapters (new files only)"`.

### Task 7: Switch over and move the tests

**Files:**
- Modify: `services/order-service/main.go` (composition root only), `services/order-service/env.go` (unchanged except nothing may import it from `internal/`)
- Delete: `cancel.go`, `db.go`, `idempotency.go`, `outcomes.go`, `pricing.go`, `promo.go` at the service root
- Move (`git mv`, test names kept): the root `*_test.go` files into `internal/integration/` and the ones with no database into the package of the code they test; `golden_test.go` is already represented by Task 6's tests and is deleted once they pass.

**Interfaces:**
- Consumes: every constructor from Tasks 3 to 6.
- Produces: `main` wiring `postgres.Connect`, `kafka.NewEncoder`, `redis.NewMultipliers/NewClaims`, `app.Pricer`, `app.PromoChecker`, `app.New`, `http.NewAPI`, `kafka.NewHandler(...).Run`, the outbox relay, the key-purge loop (a small `runKeyPurge(ctx, svc, time.Hour)` that stays in `main.go` and calls `PurgeExpiredKeys`), `health.Mount`, `serve.Run`; `order-service migrate` still works.

- [ ] **Step 1: Wire `main.go`** (keep startup order, env reading, shutdown with `background.Wait()`, the `signal.NotifyContext`, `tracing.Setup`) and delete the six flat files in the same commit; move tests with `git mv` so history follows, adapting them to enter through `NewAPI`/`Handle` and the real adapters against Postgres, Redis and Kafka.
- [ ] **Step 2: Run the whole module** with `TEST_DATABASE_URL`, `TEST_REDIS_ADDR` (not on `:6379`), `TEST_KAFKA_BROKERS`: `go test -race -count=1 -v ./...`. Expected: every test from Task 1's baseline count still passes by name (list any renamed or split test and why in the report) plus the new ones; 0 skipped.
- [ ] **Step 3: Check** `go build .` in the service directory, `go build ./...` over every module in `go.work`, and `gofmt -l`.
- [ ] **Step 4: Run e2e** four times, each against a freshly recreated Kafka (a reused broker poisons the payment-service Kafka tests; known, not fixed here). Expected: 4 passes.
- [ ] **Step 5: Commit** `git add -A services/order-service && git commit -m "order-service: wire main through the new layers and delete the flat code"` (only this directory is staged).

### Task 8: Arch test, docs and the contract proof

**Files:**
- Create: `services/order-service/internal/arch_test.go`
- Modify: `docs/adr/0014-clean-architecture-for-go-services.md` (status line and the order-service result), any doc that names the deleted files (find them with `grep -rn 'order-service/\(main\|cancel\|idempotency\|pricing\|promo\|outcomes\|db\)\.go' docs specs README.md`)

**Interfaces:**
- Consumes: `archtest.Default`, `archtest.ModulePath`.

- [ ] **Step 1: Write `TestImportsFollowTheDependencyRule`** exactly like `services/trip-service/internal/arch_test.go`, with `rules := archtest.Default(module); rules.AppAllowed = append(rules.AppAllowed, "grab-mini/pkg/surge", "grab-mini/pkg/promo"); rules.Test(t, ".")` (Decision 1), with a comment saying why those two are allowed.
- [ ] **Step 2: Run it** `go test ./internal/...`. Expected: PASS.
- [ ] **Step 3: Prove it can fail.** Add temporary files that import `net/http` into `internal/domain`, `pgx` and `grab-mini/pkg/events` into `internal/app`, and `adapters/postgres` into `internal/adapters/http`; run the test; Expected: each is reported with its reason. Delete the files and re-run; Expected: PASS. Show both outputs in the report.
- [ ] **Step 4: Contract proof.** Compare against `baseline.md`: routes and roles, metric names, labels and help, log format strings, env vars with defaults, topics, keys and `GroupID`, migration file hashes (identical), and `git diff --stat <base>..HEAD -- deploy scripts docker-compose.yml services/pkg services/e2e-test` (empty). Run `python3 deploy/k8s/generate.py --check` and `kubectl kustomize deploy/k8s/base | python3 deploy/k8s/check.py` (PyYAML in a throwaway venv, deleted after): `check.py` must still report order-service as a caller of the routing service and a Redis user (it searches `internal/` recursively). The rendered kustomize SHA equals the baseline's.
- [ ] **Step 5: Update the docs** (ADR status and what was learned; fix stale file references) and list in the final report anything the ADR's recipe missed.
- [ ] **Step 6: Run** `go test -race -count=1 ./...` for `services/order-service`, `services/pkg/archtest`, `services/payment-service` and `services/trip-service`. Expected: PASS.
- [ ] **Step 7: Commit** `git add services/order-service/internal/arch_test.go docs && git commit -m "order-service: test the dependency rule and update the docs"`.
