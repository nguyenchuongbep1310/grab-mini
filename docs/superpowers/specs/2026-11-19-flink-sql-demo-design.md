# Flink SQL demo beside ksqlDB: design

Status: approved by the user on 2026-11-19. Size: small (infrastructure profile, two SQL files, a guide section).

## Intent

The user wants Apache Flink in the project **to learn it**, not to replace anything. Today ksqlDB computes all analytics from the Kafka topics (`ksql/statements.sql`, 6 source streams and 9 derived tables) and the Ops dashboard reads it over REST. Flink will run **beside** ksqlDB on the same topics so the two engines can be compared on the same data, and so the things Flink makes visible (event time and watermarks, windows, checkpoints, the job UI) can be tried by hand.

## Goals

- `docker compose --profile flink up` starts a Flink cluster (JobManager, TaskManager) and submits two Flink SQL jobs that read the existing topics.
- Job 1 is a twin of the ksqlDB `orders_per_minute` table (orders per minute by vehicle type, tumbling event-time window), so its numbers can be compared with `SELECT * FROM orders_per_minute` in ksqlDB.
- Job 2 is a session window over `driver.location.updated`: per driver, when a burst of reports starts and ends (gap of 30 s) and how many points it held.
- Results go to new Kafka topics (`flink.orders_per_minute`, `flink.driver_sessions`) that can be read in Kafka UI.
- The Flink web UI shows the jobs, their checkpoints and watermarks.
- Checkpoints are written to a volume, so the guide can include "kill the TaskManager and watch the job resume".
- `docs/guide.md` and `docs/vi-guide.md` get a short "Stream processing with Flink (demo)" section: what to run, what to look at, three things to try.

## Non-goals

- Replacing ksqlDB or touching `ksql/statements.sql`, `ksql-init`, or the Ops dashboard.
- Java, Scala or PyFlink jobs (approach B is a later step if wanted).
- Kubernetes manifests, CI jobs, production hardening (`deploy/k8s` and `.github` stay untouched).
- A new service or any Go or TypeScript code.

## Anti-goals

- The default `make dev` becoming heavier, slower, or failing because Flink images or jars cannot be fetched. Everything Flink is behind the `flink` compose profile.
- Breaking `ksql-init`, the existing topics, or `scripts/dev.sh --reset`.
- Presenting Flink's output as an authoritative source for the product: it is a demo.

## Constraints from the project

- Kafka is reachable inside the compose network at `kafka:19092`; Schema Registry at `http://schema-registry:8081`. Ports 8080 (Kafka UI), 8081 (Schema Registry), 8088 (ksqlDB) are taken, so the Flink UI uses 127.0.0.1:8082.
- The source topics are Avro through Schema Registry (`order.created` and `driver.location.updated` carry an `occurredAt`/`timestamp` field in an envelope; ksql uses the record timestamp for windows today).
- Compose runs a single broker with replication factor 1; Flink's Kafka sinks need no more than that.
- `docker compose down -v` / `make infra-reset` must also remove the new Flink volume.
- Docs are in English with a Vietnamese twin (`docs/vi-guide.md`); both get the section.

## Approaches

| | What | Trade-off |
|---|---|---|
| **A. Flink SQL, no Java (chosen)** | Cluster in compose; two SQL scripts submitted by a one-shot container. | Least new tooling; teaches event time, windows, checkpoints and the UI; limited feel for the DataStream API. Needs the Kafka and Avro-Confluent connector jars at the right Flink version. |
| B. Java DataStream job | A new module with state and timers. | Deeper Flink, but adds a JDK, Maven or Gradle and a jar pipeline to a Go and TypeScript repo. |
| C. A then B | Both. | Best learning, twice the work. |

The user picked **A**. B stays open as a follow-up.

## Design (my calls, for the user to check)

1. **Compose profile `flink`** with `flink-jobmanager`, `flink-taskmanager` and a one-shot `flink-sql-init` that runs the SQL scripts with the Flink SQL client in application-style (`-f`), like `ksql-init` does. The Flink UI is on `127.0.0.1:8082`; JobManager RPC stays internal.
2. **A small image** at `deploy/flink/Dockerfile` built on the official `flink` image (a pinned 1.20.x tag), adding `flink-sql-connector-kafka` and `flink-sql-avro-confluent` jars of the matching version, fetched at build time with pinned URLs. This needs internet at build time; it only affects people who use the profile. The exact versions are verified when the image is first built, not assumed.
3. **SQL lives in `flink/sql/`** (`01-sources.sql`, `02-orders-per-minute.sql`, `03-driver-sessions.sql`), mirroring `ksql/statements.sql`. Sources declare the topics with the `avro-confluent` format and a watermark of 5 seconds on the event time; the choice of event-time column per topic is the builder's, taken from the Avro schemas in `services/pkg/events/schemas`.
4. **Sinks are `upsert-kafka` topics with JSON values** so they are readable in Kafka UI without a registered schema. This departs from the project's "events are Avro" rule on purpose: these topics are analytics output for the demo, not event contracts, and the guide says so. Topics are created by the job's auto-create (the broker allows it); `kafka-init` is not changed.
5. **Checkpointing** every 10 s to a `flink-data` volume (`file:///` state backend), restart strategy "fixed delay". The guide's exercise: `docker kill grab-mini-flink-taskmanager`, restart it, watch the job resume from the checkpoint and the counts continue.
6. **Make targets**: `make flink-up` (starts the profile), `make flink-sql` (opens an interactive SQL client against the cluster), `make flink-down`. They are additions; no existing target changes.
7. **Docs**: a section in `docs/guide.md` and `docs/vi-guide.md`; the Vietnamese guide's table of contents file (`docs/vi-tong-hop-tai-lieu.md`) is not changed beyond a link if needed. No ADR: this document is the record, and the guide says it is a demo.

## Left to the builder

- Exact Flink and connector versions and their download URLs (check they match each other and the JDK in the image).
- Column mapping from the Avro schemas, the event-time field per topic, and the SQL itself.
- Whether the SQL client runs best as a one-shot in compose or through the SQL Gateway.
- Memory settings for the JobManager and TaskManager so the demo fits on a laptop next to the rest of the stack (target: the profile adds well under 2 GB).

## How it is checked

1. `docker compose --profile flink up -d --build` succeeds and `flink-sql-init` exits 0.
2. Flink's REST API (`/jobs/overview` on the UI port) lists both jobs as RUNNING.
3. Run `make loadtest RATE=10 DURATION=30s DRIVERS=10` against `make dev`; for a finished minute, the count per vehicle type in `flink.orders_per_minute` equals ksqlDB's `orders_per_minute` for the same window (differences, if any, are explained in the guide: window alignment, late data, event time versus record time).
4. Kill and restart the TaskManager: both jobs return to RUNNING and counts continue.
5. `make dev` without the profile is unchanged: no Flink container starts, `ksql-init` still passes.
6. `make infra-reset` removes the Flink volume.
