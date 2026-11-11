# 0014. Layer the larger Go services with Clean Architecture, one service at a time

Status: accepted. Migrated so far: `payment-service` (the pilot), `trip-service`, `order-service`, `promotion-service` and `profile-service`; the other services follow only as listed in the migration order below.

## Context

Every Go service under `services/` is one flat `package main`. In the bigger ones the same file mixes four things: HTTP handlers, Kafka consumers, SQL, and the business rule they exist for. In `payment-service`, for example, `payments.go` decides whether a cancellation is chargeable, calls the provider, builds the ledger transaction, runs SQL inside a pgx transaction, writes the outbox row and increments Prometheus counters in one function (`settle`). The rules are correct and well tested, but:

- A rule can only be tested through Postgres (most of `payment-service`'s 54 tests need a database), because the rule and the SQL share a function.
- The cut between "what the business decides" and "how it is stored or delivered" is only visible by reading the code, so a change to a route or a table can reach a rule by accident.
- Nothing stops a rule from importing `net/http` or `pgx` tomorrow.

What must not change: HTTP routes and JSON, Kafka topics, keys, consumer groups and Avro schemas (`services/pkg/events` is the published contract), tables and migrations, environment variables, metric names and labels, log lines that the runbook and alerts reference, and the delivery guarantees of [ADR 0003](0003-at-least-once-and-idempotent-consumers.md), [ADR 0004](0004-transactional-outbox.md) and [ADR 0010](0010-payment-ledger-and-provider-idempotency.md).

### Survey (non-test lines; measured, not estimated)

| Service | Lines | Mixed in `main` | Worth the layers? |
|---|---|---|---|
| payment-service | 1431 | money, ledger, fee policy, provider, SQL + outbox, HTTP, consumer, metrics | **Yes, and the pilot.** Pure rules with real invariants (balanced ledger, no lost dong, grace period, discount); the clearest seam (provider, store, outbox); the one place where a bug costs money. |
| trip-service | 1342 | trip state machine, proximity rule, cancel under an advisory lock, SQL, HTTP, consumer | **Yes, 2nd.** The state machine and the proximity rule are pure logic buried in a 553-line `main.go`. |
| order-service | 1468 | pricing, promo token check, idempotency key store, cancel, SQL, HTTP | **Yes, 3rd.** The largest; pricing and promo rules are pure. Highest traffic, so migrate after the pattern is proven twice. |
| promotion-service | 1153 | discount maths, redemption rules, SQL, HTTP, consumer | **Yes, 4th.** `discount.go` is already almost a domain package. |
| profile-service | 1697 | field validation, driver review workflow, resync, SQL store, HTTP | **Yes, 5th**, mainly for the review workflow and `validate.go`. (1697 measured before its migration; the 1399 first written here was right when this ADR was written but went stale when the resync, commit 5da7479, was added afterwards.) |
| chat-service | 1193 | send allowance, trip lookup cache, retention, SQL, HTTP, consumer | **Maybe, 6th.** The rules (allowance, privacy) are small next to the plumbing; split only if it keeps growing. Checked 2026-11-11: still 1193 lines, unchanged since this survey, so not started. |
| pricing-service | 727 | surge maths, quotes, routing client, Redis sampling | **No, revisit (checked 2026-11-11).** The surge and quote maths is not in this service: it is `services/pkg/surge`, shared with order-service and routing-service, and it is not import-pure (`os`, `pkg/secret`), so it cannot move into one service's `internal/domain`. What is left is about 110 lines of rules in 727, roughly 15% of the service, and lifting the rest needs 7 or 8 single-implementation ports over Redis, HTTP and metrics code. Revisit when a second use case needs a port or `pkg/surge` is split. The plan that was written is kept in `docs/superpowers/plans/2026-11-11-pricing-service-clean-architecture.md`. |
| matching-service | 593 | Redis claim (Lua), eligibility review, cancel | **No full structure.** The business rule *is* one atomic Redis script ([ADR 0005](0005-redis-atomic-driver-claim.md)); putting an interface around it would move the rule out of where it is correct. At most extract the review ordering rule. |
| notification-service | 792 | WebSocket hub, Redis fan-out, consumer | **No.** It is almost all delivery infrastructure; there is no business rule to protect. |
| rating-service | 401 | stars/tip validation, SQL, HTTP, consumer | **No for now.** One file with one rule; revisit only if tips grow rules. |
| location-gateway | 408 | rate limit and dedupe tracker, Kafka writer | **No.** `tracker.go` is already a pure type with tests; a layer adds nothing. |
| routing-service | 304 | route model, HTTP | **No.** Too small and pure already. |
| auth-service | 193 | tokens, HTTP | **No.** Too small. |
| driver-location-service | 110 | one consumer loop | **No.** Too thin: there is nothing to put in a domain. |

Out of scope by design: `dlq-redrive`, `loadtest`, `driver-app-sim`, `e2e-test`, `dev-registry` (tools and tests, not services).

The rule of thumb behind the table: a service earns layers when it has (a) business rules that can be stated without I/O *and* (b) more than one kind of I/O around them. Below roughly 500 lines, or when the rule lives inside the infrastructure call (a Lua script), the interfaces would have one caller and no second implementation, which is cost without benefit.

## Decision

Services that are worth it get the following structure, and keep it checked by a test.

### Layout

```
services/<name>/
  main.go                 composition root: package main. Reads env, builds adapters, wires them, starts them.
  env.go                  environment variables (names unchanged). Stays in main.
  migrations/             SQL files stay where scripts and the Dockerfile expect them; embedded by main.
  internal/
    domain/               entities, value objects, pure rules, domain errors. Standard library only.
    app/                  use cases and the ports they need (small interfaces, declared here). No I/O libraries.
    adapters/
      http/               handlers, JSON shapes, auth middleware glue
      kafka/              consumers: decode, map to app commands, call the use case
      postgres/           the store: implements the app's ports with SQL, including the unit of work and the outbox
      <provider|redis|metrics|...>/
    integration/          black-box tests that wire the real adapters (only _test.go files)
```

`main` stays at the service root because `deploy/docker/Dockerfile` builds `services/$SERVICE` with `go build .` and the scripts look for `services/<name>/migrations`.

### Dependency rule

Source dependencies point inward: `adapters -> app -> domain`. In detail:

- `internal/domain` imports the standard library only (no `net/http`, `database/sql`, `os`), and nothing from this module except other domain packages.
- `internal/app` imports `internal/domain`, the standard library (not `net/http`, `database/sql`), and pure helper libraries on an explicit allow-list (`github.com/google/uuid` by default; a service may extend `Rules.AppAllowed` for a `services/pkg` library whose functions the app code calls are pure, as `order-service` does for `pkg/surge` and `pkg/promo`, see the order-service result below). It never imports `pgx`, `kafka-go`, `redis`, `prometheus`, or anything under `services/pkg/*` (module path `grab-mini/pkg/...`), and never an adapter.
- `internal/adapters/*` may import app, domain, the infrastructure libraries and `services/pkg/*`. An adapter never imports another adapter: only `main` (and the integration tests) join them.
- `services/pkg/*` never imports a service.

Why `app` may not import `pkg/events`: the events are the *wire contract*. A use case that took `events.TripStatusChanged` would be shaped by Avro field names. The Kafka adapter maps each event to a plain command type owned by `app` (the strings stay strings; nothing is renamed on the wire). The cost is one small mapping function per event; the benefit is that a schema change touches one adapter file.

### Enforcement

A Go test in the service, `internal/arch_test.go`, parses every non-test `.go` file under `internal/` with `go/parser` (imports only) and fails with a list of `file: imports X (rule)` when a rule above is broken. It runs under plain `go test ./...`, so CI needs no new job. The rule and the checker live in the small `services/pkg/archtest` module (a dev-time helper, imported from `_test.go` files only; it carries no business rule), with its own table test on made-up imports; each service's `internal/arch_test.go` is a few lines that name the service's module (`archtest.Default(module).Test(t, ".")`). A service that needs another layer layout or allow-list changes the `Rules` fields instead of copying the checker. A rule that is easy to bypass is not a rule, so the check is on imports, not on naming conventions.

### Ports, transactions, outbox, idempotency

- **Ports belong to the consumer.** They are small interfaces declared in `internal/app` (for payment: an order store, an assignment store, a payment lookup, a read side, a provider, a unit of work, a metrics sink). One Postgres `Store` type implements several of them; the app does not see that.
- **Unit of work.** `UnitOfWork.Do(ctx, func(Tx) error)` runs the function in one database transaction: commit if it returns nil, roll back otherwise. `Tx` exposes only what the use case writes in that transaction: mark the event processed, insert the payment, post the ledger, enqueue the outbox row. So the guarantee of ADR 0004 (state and event commit together, a relay publishes afterwards) is kept by the adapter's `Do`, and the outbox table, `outbox.Add` and `Relay.Wake` stay in the Postgres adapter. The relay itself is built in `main`.
- **Idempotency is a use-case concern.** The `eventId` dedupe and the `(trip, kind)` uniqueness are checked by the use case *inside* `Do` (ADR 0003, ADR 0010), against ports. The provider idempotency key is built by the use case from domain data. Nothing about this moves into an adapter.
- **Encoding.** The Avro encoding of the outgoing event can call Schema Registry, so it happens before the transaction opens (as today). The app hands a plain `PaymentRecorded` value to an `Encoder` port; the Kafka-side adapter turns it into the `events.PaymentRecorded` struct and Avro bytes.
- **Metrics and logs.** The use case reports facts through a small `Metrics` port (recorded, duplicate, skipped, provider error, unbalanced ledger); the Prometheus implementation is an adapter and keeps the same metric names, labels and help text. Log lines stay in the use case through the standard `log` package, character for character, because the runbook quotes them.

### What stays in `services/pkg`

The shared infrastructure kit stays as it is (`auth`, `outbox`, `kafkaconsumer`, `eventcodec`, `health`, `metrics`, `migrate`, `tracing`, `events`, ...). Business rules do not move into it; it never depends on a service. `events` is the published contract: changing it is a contract change, never a refactor.

### Go's `internal` visibility

Everything under `services/<name>/internal/` can be imported only by code in `services/<name>/`. That makes the layers private to the service: no other service can reach into `payment-service/internal/domain`, so cross-service reuse is forced through events or `services/pkg` and cannot appear by accident.

### Tests

- Domain and app rules are plain unit tests with no containers (money, split, policy, ledger validation; app use cases against in-memory fakes where the behaviour does not depend on SQL).
- The existing integration tests keep their names and what they prove. They run against real Postgres and Kafka in `internal/integration`, which wires the real adapters and enters through the Kafka `Handler` and the HTTP `API`, so what they exercise is the same path as production. (In `payment-service`: 33 tests, none renamed; one domain test that also counted a metric was split in two, see the pilot report.)
- App use cases additionally have in-memory unit tests against fakes of the ports (no containers): charged once, redelivery, decline, provider outage, early trip event, cancellation grace.
- Adapter-local tests live beside the adapter (the fake provider; the test that pins the domain's and the app's copies of wire constants to `services/pkg/events`).
- The e2e test in `services/e2e-test` is unchanged and remains the final check.

### Tooling that reads the source text

Some repository checks grep Go source instead of running it, and moving code can silently blind them. Found while migrating `payment-service`:

- `deploy/k8s/check.py` finds consumer groups with the regex `GroupID:\s*"..."` over every non-test `.go` file. The group name must therefore stay a literal in a `GroupID:` field (a named constant made the check fail with "scales on group ... which no code uses"). It scans recursively, so `internal/` is covered.
- The same script decides which services call the routing service and which use Redis by searching the `*.go` files of a service for `"ROUTING_URL"` and `redisconn.`. It used to look at the top-level files only, which would have blinded it as soon as that code moved under `internal/`; it now searches recursively (commit 91f5d2a). `order-service` neither reads `ROUTING_URL` nor ever did; its `redisconn.` call stays in the root `main.go`, and its consumer group sits in `internal/adapters/kafka/handler.go` and is found by the recursive scan (hiding `internal/` makes `check.py` fail with "scales on group 'order-service-group', which no code uses").
- `deploy/verify.sh` builds `services/<dir>` with `go build .`, so `main` stays at the service root.

### What is explicitly not changed

HTTP routes, status codes and JSON bodies; topics, keys, consumer groups and Avro schemas; tables, constraints and migration files (none renumbered, none edited, none moved); environment variable names and defaults; metric names, labels and help text; log lines that docs or alerts reference; `deploy/` and `scripts/kafka-acls.sh`.

### Result: order-service (third)

`order-service` is migrated: `internal/domain` (booking validation, order and cancel rules, idempotency key and fingerprint, access rules), `internal/app` (booking, cancel, outcomes, queries, pricing and promo decisions, ports), `internal/adapters/{http,kafka,postgres,redis,metrics}`, and `internal/integration` (the old flat integration tests, entering through the real `http.API` and kafka `Handler`). `main.go` is the composition root only (522 lines became 156); `env.go`, the migrations, `deploy/`, `scripts/`, `services/pkg/events` and the rendered Kubernetes manifests are byte-identical to before. The dependency rule is checked by `internal/arch_test.go`, which was shown to fail on a forbidden import in each layer.

What this migration taught that the recipe above did not say:

- **A pure `services/pkg` library may be allow-listed per service.** The quote and promo decision logic is the heart of booking, and it needs `*surge.Signer`, `surge.Quote` and `*promo.Signer`. The packages are not import-pure (`surge` imports `os`, `strconv`, `time`, `fmt` and `pkg/secret`; `promo` imports `pkg/secret`, which imports `os`), but `app` only calls their pure functions (`surge.Quote`, `Price`, `CellID`, `BaseFare`, `HaversineKm`, `NewSigner`; `promo.NewSigner` and the signer's methods), while the environment-reading ones (`SignerFromEnv`, `surge.CellDegrees`) are called only from `main`. So `order-service` sets `rules.AppAllowed = append(rules.AppAllowed, "grab-mini/pkg/surge", "grab-mini/pkg/promo")` in its own `arch_test.go` instead of wrapping them in a port. The shared `Default` is unchanged, and anything else under `services/pkg` (for example `pkg/auth`, `pkg/events`) is still rejected in `app`. Add an entry only if the app code calls no function of the package that does I/O or reads the environment. `archtest` checks direct imports only, so this is a review-time rule.
- **Packages that touch the same tables need their own database when `go test ./...` runs them in parallel.** The flat tests and the new Postgres adapter tests both deleted from the same tables, so one shared database made them flake (two failures in the flat package) unless run with `-p 1`. Each DB-backed package (`adapters/postgres`, `integration`) now creates a database named after itself (`ordertest_<package>_<random>`) per test and drops it `WITH (FORCE)`; no `-p 1` is needed, and a leftover database after a run is a bug.
- **Keep the golden replay test.** Before touching anything, a table of exact status, `Content-Type` and body for the error paths, and the exact bytes of the stored idempotent response, was recorded from the old code. The stored response is rendered through an `app.ReceiptEncoder` port that the HTTP adapter implements (it keeps the old sorted-key marshal), so a replay of a row written by the old binary is byte-identical. Those tests moved with the code and are the reason the use case could be split from the HTTP shape at all. Do the same for any service that stores a rendered response.
- **Transaction scope is part of the behaviour.** With an `Idempotency-Key` the transaction opens first (advisory lock, key lookup, pricing, insert); without one, pricing and Avro encoding run before a transaction that covers only the insert. The unit of work keeps both shapes, and each has a test that pins which runs inside it.
- **Release claims on every failure path, with a context that may already be cancelled.** A claimed quote or promo token is given back when the booking fails afterwards; the release must not use the request context.
- **Move tests with the code, then diff the names.** The 66 flat test names were accounted for one by one (moved with `git mv`, or deleted only where an equal test with the same name existed in a new package). A test that was deleted because it duplicated another is a decision, written down, not a side effect.
- **`go test` per package can pass while `main` panics.** Prometheus counters register at package init, so a second declaration of any of the six counters panics at start. The binary was run (`migrate` subcommand) after the flat files were deleted, and `promauto` appears in exactly one package.
- **Explicit wiring.** `Pricer` and `PromoChecker` must be non-nil in `app.Deps`; the old handler fell back to package-level defaults for tests, so `main` (and the integration helper) have to build them explicitly.

### Result: promotion-service (fourth)

`promotion-service` is migrated: `internal/domain` (code and price parsing, the create validation, the discount maths, the validate refusals and their order, the redemption decision), `internal/app` (validate, create, disable, list, the redemption and the three release use cases, ports), `internal/adapters/{postgres,metrics,promotoken,kafka,http}`, and `internal/integration` (the old flat integration tests, entering through the real `http` API and kafka `Handler`). The service root went from 8 non-test files and 1153 lines (`api.go` 364, `redemption.go` 245, `promotions.go` 141, `discount.go` 125, `consumer.go` 78, `seed.go` 44, `env.go` 41, `main.go` 115) to 2 files and 196 lines: `main.go` (155, the composition root) and `env.go` (41, unchanged). The rest is 1711 non-test lines in 25 files under `internal/`. The migrations, `env.go`, `go.sum`, `deploy/`, `scripts/`, `docker-compose.yml`, `services/pkg`, `services/e2e-test` and the rendered Kubernetes manifests are byte-identical to before. `go.mod` changed in one line: `github.com/prometheus/client_model` moved from the indirect to the direct block because an adapter test imports it. The metric names, labels and help text, the routes and their roles, the `GroupID` literal and the consumed topics are unchanged, and the set of log format strings is unchanged (the connection-failure `Fatalf` that was in three places now sits in two in `main.go`, with the third being `postgres.Migrate`, which returns the same text, wrapped, for `main` to `log.Fatal`). The dependency rule is checked by `internal/arch_test.go`, which was shown to fail on a forbidden import in each layer (`net/http` in domain; `pgx`, `pkg/events` and `pkg/promo` in app; one adapter importing another).

`app` imports no `services/pkg` package. `go list` over `internal/app` shows `github.com/google/uuid` and `internal/domain` as the only imports outside the standard library, so the shared `Default` rules were used unchanged, as in `payment-service` and `trip-service` (only `order-service` extends `AppAllowed`). `pkg/promo` was kept behind an `app.TokenIssuer` port (implemented by `adapters/promotoken`) rather than allow-listed as `order-service` did, because the package imports `pkg/secret`, which imports `os`; with the port, `app` never sees it. `internal/domain` imports only the standard library (`errors fmt math regexp strconv strings time`). These are direct-import checks, as `archtest` is: a package an allowed package imports in turn is not seen (`github.com/google/uuid` itself imports `net` and `os`), so what `app` may import stays a review-time decision as well as a test.

What this migration taught that the recipe above did not say:

- **Read the old code for when a thing happens, not what the plan says.** The plan said the relay should be woken after the commit only when an outbox row was queued. The flat code wakes it after every commit that recorded a redemption, including the already-cancelled path that queues nothing, and not after a duplicate or a release. The first version of the use case followed the plan; review caught it against the flat code, and the behaviour was restored (a test, `TestWakeAfterEveryRecordingCommit`, pins each path). A spurious relay poll is harmless, but "preserve behaviour" means the flat code is the spec, not the plan.
- **"Encode before the transaction" is not universal.** Here the Avro encode of `promotion.redeemed` runs inside the transaction, holding the advisory lock and the promotion row lock, because whether an event is encoded depends on the insert result (a duplicate or an already-cancelled order has none). The use case keeps it there, so a slow Schema Registry lengthens the lock. That is existing behaviour, kept and noted, not a fix.
- **A port error that needs a log label must not be re-wrapped by a use case.** The flat code logs a list-scan failure as `read promotion: ...` but a query failure as `list promotions: ...`. The use case wraps its own steps in `app.StepError`; the first version re-wrapped the adapter's already-labelled scan error as `list promotions: read promotion: ...`. `ListPromotions` now passes an existing `StepError` through, and the HTTP adapter tests `errors.As(*StepError)` before `errors.Is(domain.ErrNotFound)`, because a row that vanishes after a write is a 500 `load promotion`, not a 404.
- **Admin-view timestamps follow the process zone.** `validFrom`, `validTo`, `disabledAt` and `createdAt` are `timestamptz` columns that pgx scans into `time.Local`, so the JSON offset is the process zone (production containers run UTC, so `Z`). That is kept as it is: the adapters return what pgx scanned and nothing forces UTC. The golden tests pin `time.Local` to UTC for that reason.
- **Record golden tests from the old code before moving anything, including the order of refusals.** A validate request that breaks several rules at once answers with the first in a fixed order (not found, not started, expired, wrong vehicle, minimum fare, limit reached, already used), which a reordering of the checks would silently change. The first round of review of the golden tests added rows for that order, and they moved with the code unchanged.
- **When a golden test is split across layers, account for every assertion.** `TestLogLinesAreStable` was split into admin lines (http), internal-error lines (http) and event lines (app, over fakes). The assertion that a cancel seen before its order logs nothing was lost in the split and found only by review; it is now `TestAReleaseThatReleasesNothingLogsNothing`, with a mutation that fails it. Check a split by grepping each old assertion in its new home.
- **A plan's claim about a shared package can be wrong.** The `order-service` plan said `pkg/promo` was pure; it is not (see above, and the correction in the order-service result). Verify an allow-list claim with `go list` on the package, and say that it is a direct-import fact.
- **The checks that read source text kept working.** `deploy/k8s/check.py` still finds `promotion-service-group` (the literal is in `internal/adapters/kafka/handler.go`; hiding `internal/` makes it fail with "scales on group 'promotion-service-group', which no code uses"), and promotion-service is still neither a Redis user nor a routing caller (`redisconn.` and `"ROUTING_URL"` appear nowhere in it). `generate.py --check` and the `kubectl kustomize` render hash are unchanged.

### Result: profile-service (fifth)

`profile-service` is migrated: `internal/domain` (the field cleaning of `validate.go`, the driver profile and its statuses, the review actions and their allowed transitions, the review time, the resync eligibility and dating, the admin-list cursor, the domain errors), `internal/app` (riders, the driver profile save, apply, review, resync and the read use cases, ports), `internal/adapters/{postgres,metrics,kafka,http}`, and `internal/integration` (the old flat integration tests and the golden tables, entering through the real `http` API over the real Postgres adapters). The service root went from 8 non-test files and 1697 lines (`api.go` 560, `store.go` 389, `resync.go` 245, `validate.go` 147, `profile.go` 117, `main.go` 112, `seed.go` 66, `env.go` 61) to 2 files and 266 lines: `main.go` (205, the composition root and the `migrate`, `seed-dev` and `resync` subcommands) and `env.go` (61, unchanged). The rest is 2154 non-test lines in 27 files under `internal/`. The migrations, `env.go`, `deploy/`, `scripts/`, `docker-compose.yml`, `services/pkg`, `services/e2e-test` and the rendered Kubernetes manifests are byte-identical to before. `go.mod` changed in two places: `github.com/prometheus/client_model` moved from the indirect to the direct block because an adapter test imports it, and `go mod tidy` dropped `github.com/kylelemons/godebug // indirect`, which came in only through `prometheus/testutil`, which no test imports any more (`go.sum` lost its two lines). The metric names, labels and help text, the routes and their roles, the produced topic and its key (the driver id) and the outbox `source` are unchanged; the service still has no consumer group and is neither a Redis user nor a routing caller. The set of log format strings is unchanged (the connection-failure `Fatalf` that was in four places, the server, `seed-dev`, `migrate` and `resync`, now sits in three in `main.go`, with the fourth being `postgres.Migrate`, which returns the same text, wrapped, for `main` to `log.Fatal`), and the subcommands print the same lines with the same exit codes. The dependency rule is checked by `internal/arch_test.go`, which was shown to fail on a forbidden import in each layer (`net/http` in domain; `pgx`, `pkg/events` and `pkg/auth` in app; one adapter importing another).

One intended deviation: `profile-service resync` no longer builds the public rate limiter, so `RATE_LIMIT_SCALE=abc profile-service resync` now runs instead of exiting 1. The command never used the limiter; the flat code built it inside the shared service constructor, so the old failure was an accident. The server still builds the limiter before the relay starts and before the token verifier, so a bad `RATE_LIMIT_SCALE` stops the server at the same point as before. e2e was not run for this migration (the machine held its owner's own compose stack); the unit, adapter and integration suites ran against throwaway Postgres and Kafka containers.

`app` imports no `services/pkg` package. `go list` over `internal/app` shows `github.com/google/uuid` and `internal/domain` as the only imports outside the standard library, so the shared `Default` rules were used unchanged, as in `payment-service`, `trip-service` and `promotion-service` (only `order-service` extends `AppAllowed`). Three shared packages were kept out of `app` on purpose: `pkg/events` imports only `embed` and `fmt`, so it is import-pure, but it is the wire contract, so the review event's vehicle is a three-line `domain.VehicleType` pinned to `events.VehicleType` by a contract test in `adapters/kafka`; `pkg/auth` imports `net/http`, `os` and `log`, and `pkg/secret` imports `os`. `internal/domain` imports only the standard library (`errors fmt slices strconv strings time unicode unicode/utf8`). These are direct-import checks, as `archtest` is: a package an allowed package imports in turn is not seen (`github.com/google/uuid` itself imports `net` and `os`).

The resync is a use case with a lock port (`app.ResyncLocker.TryLock`, implemented by `postgres.ResyncLocker` over the same session-level advisory lock as before) and a source port for the batches of driver ids, not an adapter helper. Left in the Postgres adapter, the batching (200 drivers a transaction, after the last id), the skip of a driver whose status changed under the row lock, the dating of each repeated decision and the "a second run is refused" rule would again sit next to SQL and be testable only through Postgres; as a use case they are tested over fakes, and the adapter test only proves the lock (held on its own connection, released even when the caller's context is cancelled). `adapters/kafka` holds only the encoder (the `app.Encoder` port, mapping a plain `ReviewedEvent` to `events.DriverApplicationReviewed` and Avro bytes): the service consumes no topic, so there is no handler, and the outbox row itself is written by the Postgres unit of work.

What this migration taught that the recipe above did not say:

- **The encode runs inside the transaction here too.** As in `promotion-service`, the Avro encode of `driver.application.reviewed` runs inside the transaction, holding the driver row lock (for a resync, up to 200 of them), because the event carries the row as it is after the change. A slow Schema Registry lengthens the lock. Kept as it is and noted; the follow-up is not mechanical.
- **Where the clock is read relative to the row lock differs per use case.** The flat code reads the time before the lock when a driver saves their profile (the empty row is created with it) and once per resync batch, but after the lock and after the checks when a driver applies or an admin reviews, and only on the path that writes. A use case that "reads now first" would change the stored times under contention. Because `domain.Submit` and `domain.Decide` both check and stamp, `Apply` and `Review` call them once with a zero time to get the refusal without reading the clock, then with the real time; the order tests pin every sequence.
- **Wake conditions differ per path and are not observable in the flat tests.** The relay is woken after a profile save only when a vehicle change queued an event, after every review, after every committed resync batch even when every driver in it was skipped, and never after apply or a rider save. The flat tests never set a relay, so no golden test could see this; it was recorded by reading the code into a table before the migration and is pinned by the app order tests. The CLI passes an untyped `nil` waker (a nil `*outbox.Relay` stored in the interface would be non-nil).
- **Error labels are per handler, so no `StepError` was needed.** Every 500 in this service is logged with a label chosen by the handler (`load rider <id>`, `approve driver <id>`, `resync`, ...), never by a lower step, so the use cases return port errors unwrapped and the HTTP adapter adds the label. `promotion-service`'s `app.StepError` exists because its labels came from inside a step.
- **The HTTP-only rate limiter stayed in the adapter.** The public lookup limit is `pkg/auth`'s in-memory per-caller limiter and its refusal is a 429 with `Retry-After`; it is a transport policy with no domain meaning, so `adapters/http` builds and uses it and `app` has no port for it. A side effect is the `RATE_LIMIT_SCALE` deviation above.
- **`Cache-Control: no-store` is part of every handler response.** Profiles are personal data, so the adapter's single JSON writer sets it, and the golden tables pin the header on every row. The token verifier's own 401 and 403 (written by `pkg/auth`) do not carry it, before and after; production-design's "on every response" is slightly broader than the code, which was kept.
- **A golden table that runs over fakes needs a real-database twin.** `TestErrorResponsesAreStable`, `TestSuccessBodiesAreStable`, `TestLogLinesAreStable` and `TestCountersMoveAsRecorded` exist twice on purpose: over in-memory fakes in `adapters/http`, and over the real Postgres adapters, the real metrics registry and a real closed pool in `internal/integration` (`TestOutboxPayloadsAreStable` only there). The real-database copy is the proof that the fakes reproduce Postgres byte for byte (column defaults, list order, pgx's "closed pool" text). The cost is that a contract change is edited in two places; it cannot drift silently, because either copy fails against the code.
- **Some lock-release paths cannot be told apart from the outside.** The test that a failed advisory-lock release closes the connection instead of returning it to the pool kills the holder's backend, and pgx also closes a connection by itself after an I/O error, so the test cannot tell the explicit close from pgx's own; the branch where the unlock query answers `false` has no test without an internal seam. Recorded as a known limit.
- **Record the old answers, not the plan's expectations.** The baseline found that `resync --driver ''` means all drivers (not a refusal), that an unknown single driver on resync counts a `not_found` refusal and no run, that a public 404 moves two counters, and that an invalid UTF-8 name over JSON arrives as U+FFFD (so "must be valid UTF-8 text" is unreachable through that field). Each is pinned as recorded. The plan's claim that no file of the service mentions `GroupID` was also wrong: the Kafka integration test's throwaway reader has one, unchanged; production code has none, which is what `deploy/k8s/check.py` reads.
- **Per-test databases make the integration package slow but independent.** Each golden row that touches the database gets its own `profiletest_integration_*` database, dropped `WITH (FORCE)`, so the package takes about 30 seconds, and a leftover database after a run is a bug.

### Migration order and rollback

Per service, the same recipe, each step compiling and passing the full suite before the next: (1) record the baseline (tests, route set, metric names, log lines, k8s render); (2) move pure rules into `internal/domain` (export names, nothing else); (3) introduce `internal/app` with ports and move the use cases; (4) move SQL into the Postgres adapter; (5) move HTTP and Kafka glue into adapters and relocate tests; (6) add the import-boundary test. Order: **payment-service** (pilot) -> trip-service -> order-service -> promotion-service -> profile-service -> chat-service (only if it keeps growing) -> pricing-service (surge maths only). The others stay flat.

Rollback for any step is `git revert` of that service's commits. There is no schema, topic, config or deploy change in any step, so a deployed image can be rolled back to the previous tag at any point with no data repair. A service is migrated in its own pull request; no migration is mixed with a feature.

## Alternatives considered

- **Hexagonal naming only** (rename files to `ports.go`, `adapters.go` in `package main`). Cheap, but nothing enforces the direction of dependencies, so the next change drifts back. Rejected: the value here is the checked boundary.
- **A full DDD tactical rewrite** (aggregates, repositories per aggregate, domain events, a command bus). Rejected: it would change behaviour-bearing code in a payment path to get a vocabulary. The domain here is a handful of pure functions and value types.
- **Splitting into more microservices** (a separate ledger service, a separate provider gateway). Rejected: it turns an in-process call into a network hop and a distributed transaction, and the ledger invariant relies on one database transaction (ADR 0010). The seams belong inside the service first.
- **One shared `services/pkg/clean` framework** (base use-case and repository types). Rejected: the shared kit is infrastructure; a framework for use cases would couple every service to one shape.
- **Layer every service now.** Rejected for the small services (see the survey): empty layers are noise.

## Consequences

- Good: rules are testable without a database; the boundary is enforced by `go test`; a new adapter (a real payment provider) is one package implementing one port.
- Good: nothing observable changes, so the migration can be deployed and rolled back like any other release.
- Bad: more files and indirection (a command type and a mapping per event, a JSON shape per HTTP response in the adapter). It pays off only where the rules are rich; hence the survey.
- Bad: ports cost discipline. Interfaces with one implementation are accepted where a second implementation exists in tests (an in-memory fake, a failing provider) or where the dependency rule requires the seam.
- Bad: while services are mixed, the repository has two shapes. The ADR lists which services stay flat, so that is a decision, not a backlog.

## Revisit when

A service in the "no" rows grows a business rule that needs its own tests, or the import-boundary test needs rules that cannot be stated as import paths (then consider `go-arch-lint` or similar instead of the hand-written test).
