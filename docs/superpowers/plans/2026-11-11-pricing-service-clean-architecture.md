# pricing-service Clean Architecture Migration Implementation Plan (partial: domain only)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give `services/pricing-service` an `internal/domain` that holds the pure rules now buried in the Redis sampler, the demand counter and the quote handler, with the dependency rule enforced by `pkg/archtest`, changing no observable behaviour. **No `internal/app`, no `internal/adapters`**: the ADR 0014 verdict for this service is "Partly" and the code confirms it (Decision 1).

**Architecture:** Sixth service in the ADR order (after payment, trip, order, promotion, profile; all in `main`). The flat files `sampler.go`, `demand.go`, `quotes.go`, `routing.go` stay in `package main` and call the new domain functions; `main.go` and `env.go` do not change at all. **The surge and quote maths is not in this service**: it is in `services/pkg/surge` (Decision 2), so "lift the surge/quote maths into `internal/domain`" has nothing to lift. What is left to lift is the small set of decisions around that maths (Decision 3).

**Tech Stack:** Go 1.26 workspace (`go.work`), go-redis/v9, kafka-go (consumer, in `main.go`), Prometheus client, `miniredis` in tests, `grab-mini/pkg/{auth,surge,events,eventcodec,kafkaconsumer,redisconn,driverlocation,driverstatus,health,serve,tracing,metrics,logging,archtest}`. No database, no migrations, no outbox, no idempotency key.

**Spec:** `docs/adr/0014-clean-architecture-for-go-services.md` (survey row, dependency rule, "Small services need few layers", the Result sections). Reference shape for the arch test and the contract test: `services/profile-service/internal/arch_test.go`, `services/order-service/internal/arch_test.go`.

**Source inventory (every file read in full for this plan; measured with `wc -l` on main at 2d65302):**

| File | Lines | Holds | Domain rule inside? |
|---|---|---|---|
| `main.go` | 100 | wiring; **the only `GroupID: "pricing-service-group"`**; consumer on `order.created`; sampler goroutine; `/health`; `health.Mount` | no (stays) |
| `env.go` | 87 | `Env`, `loadEnv`, `getEnv*`; the only `"ROUTING_URL"` literal | no (stays) |
| `demand.go` | 63 | `recordDemand` (Redis TxPipeline), `demandPerMin` (MGET + average) | yes, small: unknown vehicle counts as CAR; the window average |
| `quotes.go` | 138 | metrics `pricing_quotes_total`, `pricing_fail_open_total`; `currentMultiplier`; `quoteServer`; `coord`; `handleQuote`; `routes` | yes, small: stored-multiplier validity, coordinate and vehicle validation, "surged" |
| `routing.go` | 152 | metrics (`pricing_routing_*`), `routingClient`, `distanceKm`, `failOpenReason`, `tripDistanceKm` | **no**: HTTP client plus fail-open plumbing; `failOpenReason` classifies `net.Error` (imports `net`) |
| `sampler.go` | 187 | four gauges/counters, `redisSupply`, `sampler.run/tick/refreshCell` | yes, the largest share: lease, member split, previous-value parse, drop-when-cooled, storage format, max/surging tally, supply radius |
| tests | 13 + 4 = 17 funcs, 524 lines | `pricing_test.go` (335, miniredis), `routing_test.go` (189, `httptest` fake Routing, `testutil`) | |
| **Non-test total** | **727** | matches the ADR survey row (727); **not stale** | |
| (not this module) `services/pkg/surge` | 321 non-test (`keys.go` 34, `quote.go` 161, `surge.go` 126) + 301 test | the cell, multiplier, smoothing, base fare, price, Haversine, Redis key names, the HMAC quote signer | see Decision 2 |

`services/pricing-service/pricing-service` is a 146 638-line built binary in the working directory; it is git-ignored (`.gitignore:27`). Never `git add` it; `go build .` without `-o` recreates it, so use `go build -o "$SCRATCH/pricing-service" .`.

**Measured line counts, before and expected after:**

- Before: root non-test 727, root test 524, `internal/` none.
- After (estimates, to be re-measured in Task 6): `internal/domain` about 170 non-test lines in five files and about 250 test lines; root non-test about 690 (each moved rule leaves a one-line call behind); root test about 524 plus the characterisation tests of Task 1 (the flat tests are not split, they stay and keep exercising the rules through the Redis code). `main.go` 100 and `env.go` 87 unchanged to the byte.
- The honest size of the prize: about 110 lines of rule, about 15% of the service. That is why the plan stops at the domain (Decision 1).

## Global Constraints

Copied from the ADR; every task's requirements include them.

- Dependency rule, enforced by `services/pkg/archtest` (`archtest.Default(module).Test(t, ".")` from `internal/arch_test.go`): `domain` imports the standard library only, without `net`, `net/http`, `os` and the other I/O packages in `Rules.StdlibBanned`; no `services/pkg/*`, no third-party library, no root-package symbol (a `package main` cannot be imported anyway).
- Not changed:
  - HTTP: `GET /quotes` (rider role, `auth.LimiterFromEnv(5, 20)` per rider, 429 with `Retry-After`), `GET /health`, the `health.Mount` endpoints; status codes, `Content-Type: application/json`, body bytes (`json.Encoder` trailing newline, the exact `error` texts `pickupLat, pickupLng, dropoffLat and dropoffLng are required numbers` and `vehicleType must be CAR or BIKE`, `missing bearer token`).
  - Quotes are signed (`QUOTE_SECRET`, shared with order-service): **signing is `surge.Signer.IssueForDistance` in `services/pkg/surge/quote.go` and verification is `surge.Signer.Verify` there, called by order-service's `app/pricing.go`. This plan does not touch `pkg/surge` at all**, so the quote id, its body (`b`, `m`, `p`, `e`, `r`), the 60 s TTL and the HMAC-SHA256 are byte-identical by construction; `git diff --stat main -- services/pkg` must be empty at every task. `handleQuote` keeps calling `s.signer.IssueForDistance(req, km, mult, s.now())` with the same arguments in the same order.
  - Kafka: topic `order.created`, **consumer group `pricing-service-group`** (literal stays in `main.go`: `deploy/k8s/check.py` reads `GroupID:\s*"..."` recursively over non-test `.go` files, and KafkaUser, ACL script `scripts/kafka-acls.sh` and the KEDA trigger name it), `RetryTiers: kafkaconsumer.DefaultRetryTiers`, `CommitInterval` 100 ms. No producer, no outbox.
  - Redis wire format: key names (`surge:mult:{vehicle}:{cell}`, `surge:demand:{vehicle}:{cell}:{minute}`, `surge:cells` with members `vehicle|cell`, `surge:sampler:lock`), TTLs (`surge.MultTTL` 30 s, `surge.DemandTTL` 5 min, `cellsSetTTL` 1 h), the value format of a multiplier (`fmt.Sprintf("%.1f", next)`), the `NX` lock with lease `max(SampleInterval-1s, 1s)`. order-service reads `surge:mult:*`; a drifting format is an incident.
  - Environment variable names and defaults (`SCHEMA_REGISTRY_URL`, `REDIS_ADDR`, `KAFKA_BROKERS`, `PORT`, `INSTANCE_ID`, `ROUTING_URL` with its unset-versus-empty meaning, `ROUTING_TIMEOUT_MS`, `SURGE_K`, `SURGE_MAX`, `SURGE_ALPHA`, `SURGE_MAX_STEP`, `SURGE_WINDOW_MINUTES`, `SURGE_SAMPLE_SECONDS`, `SURGE_CELL_DEGREES`/`GEO_SHARD_DEGREES`, `QUOTE_SECRET[_FILE]`, `APP_ENV`, the `auth` variables). `env.go` is not edited.
  - Metric names, labels and help text of all **eight** series: `pricing_quotes_total{vehicle_type,surged}`, `pricing_fail_open_total`, `pricing_routing_fail_open_total{reason}`, `pricing_routing_duration_seconds`, `pricing_surge_multiplier_max{vehicle_type}`, `pricing_surging_cells{vehicle_type}`, `pricing_sample_errors_total`, `pricing_ticks_total`. Alert rules in `deploy/monitoring` and the runbook quote them. They stay declared where they are (`quotes.go`, `routing.go`, `sampler.go`); the domain never imports `prometheus`.
  - Log message texts (`surge multiplier read failed, using 1.0: %v`, `surge sample failed`, `surge sample %s: %v`, `routing distance unavailable, ...`, `ROUTING_URL is empty: ...`): the runbook quotes them.
  - `deploy/`, `scripts/`, `docker-compose.yml`, `services/pkg/**` (including `events` and `surge`), `services/e2e-test`: byte-identical. `go build .` in the service root is what `deploy/docker/Dockerfile` (`go build ... -o /out/app .`) and `services/e2e-test/e2e_test.go:184` run, so `main.go` and `env.go` stay at the root in `package main`.
- **The flat code is the spec, the plan is not.** Where this plan and the code disagree the code wins and the executor says so in the report. Every task starts by re-reading the flat function it changes, line by line. **Ordering is behaviour** in `tick` and `refreshCell` (see "Behaviours to pin"); moving a statement across a Redis call or a counter changes what a failing Redis does.
- Tests:
  - Domain tests need no containers and no Redis.
  - **The service's tests need no infrastructure**: `grep -n "TEST_REDIS_ADDR\|6379\|FlushDB" services/pricing-service/*_test.go` is empty today, every Redis test builds its own `miniredis.RunT(t)` (a private in-process server per test, `newRedis` in `pricing_test.go`), and the `go test ./...` of CI (`go (services/pricing-service)` job, with a Redis service container it does not use) and `make test-integration` (`TEST_REDIS_ADDR`) pass whether or not Redis exists. So the private-resource-per-test rule is already met, **no throwaway container is needed for this service's unit run**, and no new test may use a shared Redis, `:6379` or `FlushDB`. The only containerised check is the e2e (Task 6).
  - Moved or kept tests keep their names; no test is dropped. **A golden test that is split across layers is accounted for assertion by assertion** (Decision 6): this plan splits none, but Task 2 to 4 each add a domain test beside an unchanged flat test, and the report lists for each flat assertion which test now pins it.
- Prometheus metrics register at package init (`promauto`); no task moves or duplicates a declaration, so a double registration cannot happen. Task 6 still starts the binary once.
- One service, one pull request, one commit per task. Rollback is `git revert` of this service's commits; nothing in `deploy/` changes.
- Do not fix unrelated bugs: list them in the task report (candidates in "Known, not fixed").
- Shared repository: before each commit run `git status` and `git log`; stage only your own paths (`git add <paths>`, never `-A`, `-a`, `stash` or `reset --hard`); never push.

## Decisions this plan makes (the executor must not reopen them)

1. **Domain only: no `internal/app`, no `internal/adapters`; the sampler, the demand counter, the quote handler and the routing client stay in `package main`.** Evidence: there is no store to abstract and no second implementation. All three entry points (consumer, ticker, HTTP) use the same one Redis client and the same key helpers from `pkg/surge`; a port set for them would be `DemandCounter`, `MultiplierStore`, `SamplerLock`, `SupplyCounter`, `CellSet`, `Router`, `QuoteSigner` plus metrics, seven or eight interfaces with exactly one implementation each, over about 400 lines of Redis, HTTP and Prometheus code. Their tests would then be fakes that no longer prove what the tests prove today (`TestSamplerRaisesTheMultiplier...AndStoresItWithATTL` asserts a real key, a real TTL and a real `FastForward` expiry on miniredis; `TestOnlyOneInstanceSamplesPerInterval` asserts a real `SETNX`). The ADR's own rule is "do not add a layer or an interface that has one caller and no second implementation or test double"; its trigger for revisiting is "until a second use case needs a port". Nothing in the code is a second use case that needs a different port. What it would cost if the trigger fires: a use case type per entry point (`RecordDemand`, `RefreshSurge`, `IssueQuote`) and the ports above; Task 2 to 4 are written so the domain functions become the use cases' inner calls unchanged. The order-service lesson (use cases own the transaction) does not apply: there is no transaction. **Revisit when** a second consumer or a store other than Redis appears, or when `tripDistanceKm` and the multiplier read grow into a quote use case with its own tests over fakes.
2. **The surge and quote maths stays in `services/pkg/surge`; it is not moved or copied into the service's domain.** Evidence (all from `grep` and `go list` on main):
   - `pkg/surge` is imported by `order-service` (`internal/app/pricing.go`, `errors.go`, `adapters/http`, `adapters/redis`, `main.go`), by `routing-service/model.go` (`HaversineKm`) and by this service. It is the shared contract that makes a quote signed here verifiable there, and the Redis key names and TTLs here are what order-service reads. Moving it under `pricing-service/internal` would not even compile for the other two (Go `internal` visibility, ADR "Go's internal visibility"); copying it creates two formulas for one price.
   - It is **not import-pure**: `go list -f '{{join .Imports " "}}' ./surge` in `services/pkg` prints `crypto/hmac crypto/sha256 encoding/base64 encoding/json errors fmt grab-mini/pkg/secret math os strconv strings time`. `os` comes from `surge.CellDegrees` (reads `SURGE_CELL_DEGREES`, `GEO_SHARD_DEGREES`) and `pkg/secret` (imports `errors fmt os strings`) from `SignerFromEnv`. Both are called only from `env.go` and `main.go`. Every other function in the package (`CellID`, `CellCenter`, `Round1`, `Multiplier`, `Smooth`, `HaversineKm`, `BaseFare`, `Price`, the key builders, `Signer.Issue*`, `Verify`) is stdlib-only and a table test (`surge_test.go`, 301 lines).
   - `archtest` bans `grab-mini/pkg/*` from domain. order-service solved the same problem for `app` by `rules.AppAllowed = append(..., "grab-mini/pkg/surge", ...)`. For a **domain** that would weaken the innermost layer's rule to save about three call lines (`surge.Multiplier`, `surge.Smooth`, `surge.CellID`), so the decisions around the maths are written to take its results as arguments (Decision 3) and the calls stay in `sampler.go`.
   - Not done here, and the only way to get "maths in domain" truthfully: split `pkg/surge` into an import-pure part and an env/secret part. That is a change to a shared package used by three services, outside "one service at a time", and it would not make the maths pricing-service's. Recorded as a possible follow-up, not a task.
3. **What is domain and what is plumbing.** Domain (pure, stdlib only, each a few lines; the full list is the file map below): the window normalisation and the demand average over the present buckets; the unknown-vehicle-is-CAR rule; the two readings of a stored multiplier; the storage format of a multiplier; the "cooled off" rule that drops a cell; the sampler lease; the split of a `vehicle|cell` member; the max and surging tally that feeds the two gauges; the equal-area supply radius; coordinate and vehicle validation of a quote request; "surged". Plumbing (stays in `main`): every Redis call, the two timeouts, `SETNX`, `MGET`, TxPipeline, the `GEO` search, the routing HTTP client, fail-open counters and logs, `Env`, the Prometheus declarations, the Avro decode.
4. **The two readers of one Redis key keep their two different rules.** `currentMultiplier` (quotes) accepts a stored value only if it parses, is `>= 1`, and is neither NaN nor Inf, otherwise 1.0. `refreshCell` (sampler) takes any value that parses (including `"NaN"`, `"Inf"`, `"-3"`, `"0.2"`) as `prev` and lets `surge.Smooth` clamp it (`prev < 1` becomes 1; NaN and Inf are not clamped by `Smooth`'s `prev < 1` test, and the result goes through `clamp` and `Round1`). They are two domain functions with two names, `ReadMultiplier` and `PreviousMultiplier`; unifying them changes behaviour. Task 1 pins the sampler side, which has no test today.
5. **Domain gets its own `Vehicle` type; `pkg/events` stays out.** `events` is import-pure (`embed fmt`) but it is the wire contract, and `archtest` bans `pkg/*` from domain anyway. `domain.Vehicle` is a string with `Valid()`, `Car` and `Bike`; a contract test in the root package (`contract_test.go`, which may import `events`) pins `string(domain.Car) == string(events.VehicleCar)`, the same for `Bike`, and `Valid` on `""`, `"car"`, `"TRUCK"`. The metric label values `CAR`/`BIKE` and the map keys of the tally come from these strings.
6. **Test split rule.** The flat tests stay in the root package and keep going through `recordDemand`, `sampler.tick`, `handleQuote`; they are the golden tests and none is edited except where a symbol they call was renamed (none is, by design: every flat function keeps its name and signature). Each domain function gets its own table test, written from the **old** behaviour recorded in Task 1, and the task report maps each flat assertion to the domain test or flat test that still holds it (for example `TestQuoteIgnoresAGarbageStoredValue` cases `"abc"`, `"0.2"`, `"-3"`, `"NaN"` map to `domain.TestReadMultiplier` rows with the same four inputs, and the flat test stays as the proof over Redis).
7. **Where each thing lives after.**

   | Thing | Lives in | Stays byte-identical because |
   |---|---|---|
   | Quote signing (`IssueForDistance`, HMAC, body `b/m/p/e/r`) | `services/pkg/surge/quote.go`, called from `quotes.go` `handleQuote` | `pkg/` untouched |
   | Quote verification (`Verify`) | `services/pkg/surge/quote.go`, called by order-service | `pkg/` untouched; the flat test `TestQuoteAppliesTheCellMultiplier` still verifies a quote with the shared secret |
   | Multiplier formula, smoothing, base fare, price, Haversine, cell id | `services/pkg/surge/surge.go` | `pkg/` untouched |
   | Redis key names, TTLs, `Minute` | `services/pkg/surge/keys.go` | `pkg/` untouched |
   | Consumer group literal | `main.go` | not edited |
   | `ROUTING_URL` literal, defaults | `env.go` | not edited |
   | Metrics declarations | `quotes.go`, `routing.go`, `sampler.go` | not moved |

## File map (target)

```
services/pricing-service/
  main.go env.go                      unchanged (byte-identical)
  demand.go quotes.go sampler.go      call into internal/domain; same names and signatures
  routing.go                          unchanged
  pricing_test.go routing_test.go     unchanged except additions (Task 1) and no renames
  contract_test.go                    new: domain strings pinned to events and surge (Tasks 3, 4)
  internal/
    arch_test.go                      archtest, MinFiles = 4
    domain/
      vehicle.go      Vehicle, Car, Bike, Valid, OrCar
      multiplier.go   ReadMultiplier, PreviousMultiplier, FormatMultiplier, Surged, Cooled
      demand.go       Window, AverageDemand
      sampling.go     Lease, SplitMember, Tally (NewTally, Record, Max, Surging), SupplyRadiusKm
      request.go      Coordinate, ParseVehicle (+ the two bound constants 90 and 180 as LatLimit, LngLimit)
      *_test.go       one per file
```

## Behaviours to pin before moving anything (found by reading, not covered by a flat test)

Each is recorded from the old code in Task 1 and must keep its recorded answer:

1. `refreshCell` `prev`: stored `"abc"` gives prev 1.0; stored `"0.2"` gives prev 0.2 (then `Smooth` floors it); stored `"2.0"` gives 2.0; a Redis error other than `redis.Nil` aborts the cell with the error (counted in `pricing_sample_errors_total`, cell kept).
2. `tick`: a malformed member (no `|`) is `SREM`ed and skipped without touching `pricing_sample_errors_total`; the lease is `SampleInterval - 1s` floored at 1 s (a 5 s interval gives 4 s, a 2 s interval gives 1 s, a 500 ms interval gives 1 s); a lost lock returns the `SetNX` error or nil and does not increment `pricing_ticks_total`.
3. `tick` gauges: `pricing_surge_multiplier_max` starts at 1 and `pricing_surging_cells` at 0 for `CAR` and `BIKE` on every tick, so both series exist even with no cells; a member with another vehicle string adds its own series (the maps are keyed by whatever the member says). A failed cell contributes to neither.
4. `refreshCell` drop rule: `next <= 1.0 && demand == 0` deletes the multiplier key and `SREM`s the member, and returns `(1.0, err of the pipeline)`. `next <= 1.0` with demand above 0 stores `"1.0"`.
5. Stored format: `%.1f` of `next` (so `1.25` can never occur: `Round1` runs first; `"3.0"` for a ceiling).
6. `demandPerMin`: `window < 1` means 1; a bucket missing or not a string is skipped; a bucket that does not parse as a float is skipped; the divisor is the window, not the number of present buckets.
7. `recordDemand`: an invalid vehicle (`""`, `"car"`) counts as CAR in the key and the set member.
8. `coord`: exactly one value for the name; NaN, `Inf` and `|x| > limit` rejected; `-0`, `1e1`, `+5` accepted (they parse).
9. `handleQuote` order: auth (401) first, then the four coordinates (400), then the vehicle (400); unknown vehicle text is checked **after** the coordinates, so a request with both faults answers the coordinate message.
10. `redisSupply` radius: `deg * 111.32 / sqrt(pi)` km, search limit 1000.

## Tasks

### Task 1: Baseline and characterisation tests (owner: test-writer)

**Files:** modify `services/pricing-service/pricing_test.go` (additions only); create nothing else.

- [ ] Record the baseline in the report: `cd services/pricing-service && go vet ./... && go test -count=1 ./...` (miniredis, no containers); `go list -f '{{join .Imports " "}}' .` and the `go list` of `../pkg/surge` quoted in Decision 2; `grep -rn 'GroupID' services/pricing-service --include='*.go'` (expect `main.go` only); `python3 deploy/k8s/check.py` output; `git diff --stat` of `services/pkg` is empty; the metric names with `grep -n 'Name:' *.go` (eight); the e2e baseline (below).
- [ ] Add characterisation tests for behaviours 1 to 10 that no flat test reaches, **recorded by running them against the unmodified code first** and writing the observed value, not the expected one: the stored-value cases of `refreshCell` (behaviour 1, including `"NaN"`, whose recorded result must be written down as observed), the malformed member and the lease floor (2; use a seam-free check: `rdb.TTL` of `lockKey` after a tick with `SampleInterval` 5 s, 2 s and 500 ms), the gauges through `testutil.ToFloat64(surgeMax.WithLabelValues("CAR"))` (3; counters are process-global, so compare deltas, as `routing_test.go` does), the drop rule with demand above zero (4), the `%.1f` format (5), the window cases (6), `coord` edge cases through `handleQuote` (8), the 9 ordering (a request with a bad coordinate and `vehicleType=TRUCK` answers the coordinate text), and the radius through a fake `driverlocation` only if it is reachable without a seam; otherwise record it in "could not verify".
- [ ] Requirements: no production file changes; every added test passes on the unmodified code; none uses `:6379`, `FlushDB` or a shared Redis (each builds `newRedis(t)`).
- Verify: `go test -count=1 -race ./...` in the service twice; `git diff --stat` shows `pricing_test.go` only.
- Rollback: revert the commit.

### Task 2: `domain/multiplier.go` and `domain/demand.go` (owner: go-service-dev)

**Files:** create `internal/domain/{multiplier,demand}.go` and their `_test.go`; modify `quotes.go` (`currentMultiplier`), `sampler.go` (`refreshCell`), `demand.go` (`demandPerMin`).

- [ ] Re-read `currentMultiplier`, `refreshCell` and `demandPerMin` line by line first.
- [ ] `domain.ReadMultiplier(stored string) float64` (behaviour of `currentMultiplier` after the Redis read: parse, `< 1`, NaN, Inf give 1.0); `domain.PreviousMultiplier(stored string) float64` (parse error gives 1.0, nothing else checked; Decision 4); `domain.FormatMultiplier(m float64) string` (`fmt.Sprintf("%.1f", m)`); `domain.Surged(m float64) bool` (`m > 1`); `domain.Cooled(next, demand float64) bool` (`next <= 1.0 && demand == 0`); `domain.Window(n int) int` (`n < 1` gives 1); `domain.AverageDemand(present []string, window int) float64` (skips unparsable strings, divides by `Window(window)`).
- [ ] In the flat code the Redis calls, the `redis.Nil` and error branches, the timeout, `failOpen.Inc()` and the log stay exactly where and in the order they are; only the pure expression is replaced by the call. `demandPerMin` builds `present` from the `MGet` result with the same skip of non-string values, then calls `AverageDemand`.
- Tests: domain table tests built from the recorded behaviours 1, 4, 5, 6 and the flat `TestQuoteIgnoresAGarbageStoredValue` inputs; flat tests unchanged.
- Verify: `go test -count=1 -race ./...` in the service; `go list -f '{{join .Imports " "}}' ./internal/domain` prints standard-library packages only (`fmt math strconv` expected); `git diff --stat main -- services/pkg` empty.
- Rollback: revert the commit.

### Task 3: `domain/sampling.go` and `domain/vehicle.go` for the sampler and the demand counter (owner: go-service-dev)

**Files:** create `internal/domain/{sampling,vehicle}.go` and tests; create `contract_test.go`; modify `sampler.go` (`tick`, `redisSupply`) and `demand.go` (`recordDemand`).

- [ ] Re-read `tick`, `run`, `redisSupply` and `recordDemand`.
- [ ] `domain.Lease(interval time.Duration) time.Duration`; `domain.SplitMember(member string) (vehicle, cell string, ok bool)` (the `strings.Cut` on `"|"`); `domain.Tally` with `NewTally()` (CAR and BIKE at 1 and 0), `Record(vehicle string, next float64)` (identical comparisons, unknown vehicle allowed) and accessors that `tick` iterates to set the gauges (same two `Set` calls per vehicle); `domain.SupplyRadiusKm(deg float64) float64`; `domain.Vehicle` and `OrCar`.
- [ ] `tick` keeps its order: `SetNX`, `sampleTicks.Inc()`, `SMembers`, one `now`, the member loop (`SRem` of a malformed member, `refreshCell`, error counter and log, tally), then the gauges. `refreshCell` keeps: demand read, supply (cached per cell in `supplyByCell`), previous value, `surge.Multiplier`, `surge.Smooth`, drop or `SET` with `surge.MultTTL`. The `surge` calls stay in `sampler.go` (Decision 2).
- [ ] `contract_test.go` (root package): `domain.Car/Bike` equal `events.VehicleCar/VehicleBike` as strings; `domain.Vehicle(x).Valid()` equals `events.VehicleType(x).Valid()` for `""`, `CAR`, `BIKE`, `car`, `TRUCK`; for sample vehicle and cell strings, `domain.SplitMember(surge.CellMember(v, c))` returns `v, c, true`.
- Verify: as Task 2, plus the Task 1 gauge and lease tests pass unchanged; `go vet ./...`.
- Rollback: revert the commit.

### Task 4: `domain/request.go` for the quote handler (owner: go-service-dev)

**Files:** create `internal/domain/request.go` and test; modify `quotes.go` (`coord`, `handleQuote`).

- [ ] Re-read `coord` and `handleQuote`; behaviour 8 and 9 are the spec.
- [ ] `domain.Coordinate(values []string, limit float64) (float64, bool)`; `domain.LatLimit = 90`, `domain.LngLimit = 180`; `domain.ParseVehicle(raw string) (Vehicle, bool)` (`""` gives `Car`; invalid gives false). `coord` becomes a call (or is deleted and `handleQuote` calls `domain.Coordinate(q[name], limit)`; either way the 401, then coordinates, then vehicle order, the two error texts and `auth.Deny` stay in `quotes.go`).
- [ ] `quotesIssued.WithLabelValues(req.Vehicle, surged)` uses `domain.Surged(mult)` for the label: `"true"` or `"false"` exactly as before.
- Tests: domain table for `Coordinate` and `ParseVehicle` including `-0`, `1e1`, `+5`, `NaN`, `Inf`, `91`, two values for one name, empty; flat `TestQuoteValidation` unchanged and passing; the Task 1 ordering test passing.
- Verify: as Task 2; additionally `TestQuoteAppliesTheCellMultiplier` still verifies the signed quote with the shared secret.
- Rollback: revert the commit.

### Task 5: Architecture test (owner: go-service-dev)

**Files:** create `internal/arch_test.go`.

- [ ] Same shape as `services/profile-service/internal/arch_test.go`, package `arch`, with `module, err := archtest.ModulePath("../go.mod")`, then:

  ```go
  rules := archtest.Default(module)
  rules.MinFiles = 4 // five domain files; the shared default of 10 expects a larger service
  rules.Test(t, ".")
  ```

  Comment: only `domain` exists; `app` and `adapters` are intentionally absent (Decision 1); the check sees direct imports only.
- [ ] Show the rule fails: temporarily add `import _ "os"` to a domain file, then `import _ "grab-mini/pkg/surge"`, then `import _ "github.com/redis/go-redis/v9"`; each must make `go test ./internal/...` fail with the matching message; revert each (report the three failures; nothing left behind in the commit). Also run the test from a wrong directory (`cd internal/domain && go test ../...` is fine, but `archtest.Scan` on an empty directory) to see the `MinFiles` guard fire.
- Verify: `go test -count=1 ./...` in the service passes; `git diff` of the commit is `internal/arch_test.go` only.
- Rollback: revert the commit.

### Task 6: Verification, docs and ADR (owner: go-service-dev; docs by the architect or the same agent)

- [ ] Whole-service verification: `go build -o "$SCRATCH/pricing-service" . && go vet ./... && go test -count=1 -race ./...` in `services/pricing-service`; `gofmt -l .` empty; `go list -f '{{join .Imports " "}}' ./internal/domain` (stdlib only: write the exact list in the ADR Result); re-measure `wc -l` of root and domain, non-test and test, and put the numbers in the ADR Result (replace the estimates above).
- [ ] Run the binary once to prove no double metric registration and the same startup: `ROUTING_URL= APP_ENV=development REDIS_ADDR=127.0.0.1:<port> ... "$SCRATCH/pricing-service"` against a throwaway Redis and Kafka (names `pr-mig-redis`, `pr-mig-kafka`, ports in `55xxx`, removed with `docker rm -f` afterwards), `curl /health`, `/readyz`, `/metrics | grep -c '^pricing_'` equal to the baseline; stop it with SIGTERM and check the `pricing-service stopped` line.
- [ ] e2e: `cd services/e2e-test && go test -count=1 ./...` with Kafka freshly recreated (a reused broker poisons the payment-service Kafka tests; known), the rider quote step (`e2e_test.go:435`) must pass. If the machine holds the owner's own compose stack, do not stop it: use throwaway containers or report "e2e not run" as profile-service did.
- [ ] Tooling defaults checked (lesson from earlier final reviews), each with its command and result in the report:
  - `python3 deploy/k8s/check.py` output equals the Task 1 baseline (it reads `GroupID:\s*"..."`, `"ROUTING_URL"` and `redisconn.` over non-test `.go` files recursively; `main.go` and `env.go` did not move, so the readings are unchanged, and the new `internal/domain` files contain none of the three strings: `grep -rn 'GroupID\|ROUTING_URL\|redisconn' internal` is empty).
  - `deploy/k8s/generate.py` line 75 (`"pricing-service": dict(port=3007, kind="api", db=False, redis=True, quote=True, scale=("lag", 2, 6))`) and `CALLS` line 83 stay true (no DB, uses Redis, calls routing-service, scales on consumer lag); regenerate the manifests and `git diff --stat deploy/` must be empty.
  - `Makefile`: `SERVICES` (line 5; the image list, the module list comes from `go.work` which already has `./services/pricing-service`), `metrics-check` (`pricing-service:3007`), `test` and `test-integration` loops over `GO_MODULES` (no per-service path, so nothing to change; `internal/` is covered by `go test ./...`). Not in `MIGRATE_SERVICES` and must not be added (no migrations).
  - `.github/workflows/ci.yml`: the per-module jobs derive the module list from `go.work` and run `go test -v ./...` in the module directory; the new package is covered with no change. The Redis service container there is unused by this module and stays.
  - `deploy/docker/Dockerfile` (`cd services/$SERVICE && go build ... -o /out/app .`): `internal/` is copied with the service directory; confirm with `docker build --build-arg SERVICE=pricing-service` only if Docker is available, else report not run.
  - `scripts/dev.sh` and `scripts/kafka-acls.sh` (`[pricing-service]="pricing-service-group=order.created"`): untouched; `git diff --stat scripts deploy` empty.
  - `git diff --stat main -- services/pkg services/e2e-test deploy scripts docker-compose.yml services/pricing-service/main.go services/pricing-service/env.go` empty.
- [ ] Docs, only where they became wrong:
  - `docs/adr/0014-clean-architecture-for-go-services.md`: status line adds `pricing-service` (partial, domain only); add `### Result: pricing-service (sixth, partial)` with the layout, the measured numbers, the verdict on the survey row (the 727 was right when measured and still is; the row's "surge/quote maths" was a misreading: the maths is `pkg/surge`, so the domain holds the decisions around it), the `go list` evidence of Decision 2, the Decision 4 finding (two readers, two rules), and the lessons below; update the survey row to say "Partly, done: domain only, maths stays in `pkg/surge`" and the order sentence at "Migration order and rollback" ("... -> pricing-service (surge maths only)") to match.
  - `docs/vi-huong-dan-hoc-system-design.md` lines 52, 79 and 244 link `pricing-service/quotes.go`, `routing.go:134` and `sampler.go:108`; no file moved, but line numbers shift in `quotes.go` and `sampler.go`: re-measure `tripDistanceKm` (routing.go, unchanged at 134) and the `SetNX` line of `sampler.go` and fix the numbers. (`grep -rn 'pricing-service/' docs README* specs` is the sweep; `specs/ride-booking-mini/design.md:101` names the group and the key, not a file.) Also check the developer guide in English if the Vietnamese one has an English twin (`ls docs`).
  - `docs/runbook.md` names metrics, alerts and log lines only: unchanged; confirm with `git diff`.
  - `services/pricing-service/main.go` doc comment names `currentMultiplier` and `tripDistanceKm`, both still in place: no edit.
- Verify: all of the above in the report; `git status` shows only the paths of this task.
- Rollback: revert the commit.

## Task sizes and order

Tasks 1 to 6 are sized for one implementer each; 2, 3 and 4 are independent of each other after Task 1 and can be taken in any order, each leaving the tree green. Task 5 needs at least one domain file; place it last so `MinFiles = 4` is true. Do not merge 2 to 4 into one commit: the Task 1 tests are what makes each safe, and a failure in `tick` should point to one commit.

## Review focus (what the reviewer should look for)

- **No new interface, no `app` directory, no port**: `ls services/pricing-service/internal` is `arch_test.go` and `domain`.
- Each domain function is a pure copy of the old expression: diff the old expression against the new body by eye, then by the Task 1 characterisation tests.
- `ReadMultiplier` and `PreviousMultiplier` are still two functions with two rules (Decision 4).
- The order of statements in `tick` and `refreshCell` and the place of every counter increment and log.
- `git diff --stat main -- services/pkg deploy scripts` is empty; `main.go` and `env.go` untouched.
- The domain has no import beyond `fmt math strconv strings time` (check with `go list`, not by reading).

## Known, not fixed (do not fix in this migration)

- `recordDemand` counts a redelivered `order.created` twice (documented in its comment: noise at this scale).
- `PreviousMultiplier` accepts `NaN`, `Inf` and negative stored values, which `ReadMultiplier` refuses; a corrupted value in `surge:mult:*` can therefore seed a NaN into `Smooth` (its `prev < 1` test is false for NaN). Only reachable by writing the key by hand; recorded as observed in Task 1, not changed.
- `getEnvInt` rejects zero and negatives silently (falls back to the default), `getEnvFloat` accepts NaN: left as is.
- `pricing_quotes_total` counts with `surged="true"` only when `mult > 1`, so a stored `1.0` is never "surged": intended.
- The `health.Redis`/`health.Kafka` readiness checks and the `/health` route duplicate each other in purpose: untouched.
- The 146 MB-class build artefact `services/pricing-service/pricing-service` sits in the working tree (ignored); not ours to delete.
- Kafka must be freshly recreated before every e2e run.

## Could not verify when this plan was written

- Not run: e2e, the binary against Redis and Kafka, `docker build`, `check.py` after the change; the plan only ran `go test ./...` in the service (passes on main, 17 tests, miniredis) and `go list` / `wc -l` / `grep` for every number quoted.
- `redisSupply`'s radius and its `driverlocation.Search` call cannot be exercised without a Redis GEO fixture or a seam; if Task 1 cannot reach it, the radius is pinned only by the domain unit test of `SupplyRadiusKm` and by review.
- The final domain line counts are estimates until Task 6 measures them.
- Whether the owner wants this partial migration at all: see the open question in the hand-back (the whole prize is about 110 lines of rules).
