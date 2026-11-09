# 0014. Layer the larger Go services with Clean Architecture, one service at a time

Status: accepted. Migrated so far: `payment-service` (the pilot), `trip-service` and `order-service`; the other services follow only as listed in the migration order below.

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
| profile-service | 1399 | field validation, driver review workflow, SQL store, HTTP | **Yes, 5th**, mainly for the review workflow and `validate.go`. |
| chat-service | 1193 | send allowance, trip lookup cache, retention, SQL, HTTP, consumer | **Maybe, 6th.** The rules (allowance, privacy) are small next to the plumbing; split only if it keeps growing. |
| pricing-service | 727 | surge maths, quotes, routing client, Redis sampling | **Partly.** Lift the surge/quote maths into `internal/domain`; leave the Redis sampler and routing client as they are until a second use case needs a port. |
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
