# Flink SQL Demo Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Run Apache Flink beside ksqlDB on the existing Kafka topics, behind a compose profile, with two Flink SQL jobs whose output can be compared with ksqlDB and inspected in the Flink UI, for learning.

**Architecture:** A `flink` compose profile adds a JobManager, a TaskManager and a one-shot `flink-sql-init` that submits SQL from `flink/sql/` to the session cluster (like `ksql-init` does for ksqlDB). Job 1 mirrors the ksqlDB `orders_per_minute` table; job 2 is a session window over driver locations. Sinks are new JSON topics. Nothing in the default stack, `ksql/`, the apps, the services or `deploy/k8s` changes.

**Tech Stack:** Docker Compose, Flink 1.20.x (official `flink` image) with the `flink-sql-connector-kafka` and `flink-sql-avro-confluent` jars, Flink SQL, the existing Kafka (`kafka:19092`) and Schema Registry (`http://schema-registry:8081`).

**Spec:** `docs/superpowers/specs/2026-11-19-flink-sql-demo-design.md` (approved 2026-11-19; goals, non-goals, anti-goals and the design calls it lists bind every task).

## Global Constraints

From the spec, verbatim where exact:

- Everything Flink sits behind the compose profile `flink`. `docker compose up` without it starts no Flink container and builds no Flink image.
- Flink web UI on `127.0.0.1:8082` (8080, 8081 and 8088 are taken). Inside the network the JobManager is `flink-jobmanager:8081`.
- Container names follow the repo: `grab-mini-flink-jobmanager`, `grab-mini-flink-taskmanager`, `grab-mini-flink-sql-init`. Image `grab-mini/flink:1.20` built from `deploy/flink/Dockerfile`.
- SQL lives in `flink/sql/` (`01-sources.sql`, `02-orders-per-minute.sql`, `03-driver-sessions.sql`). Sink topics: `flink.orders_per_minute` (`upsert-kafka`) and `flink.driver_sessions` (append-only `kafka`), JSON values, so they are readable in Kafka UI. They are demo output, not event contracts, and the guide says so.
- Checkpoints every 10 s to a `flink-data` volume (`file:///opt/flink/checkpoints`, mounted on the JobManager and the TaskManager), restart strategy fixed-delay.
- Memory: the profile adds well under 2 GB next to the rest of the stack.
- Make targets added: `make flink-up`, `make flink-down`, `make flink-sql`; no existing target changes behaviour except that `infra-down` and `infra-reset` also cover the profile (Task 1).
- Do not touch: `ksql/statements.sql`, `docker-compose.yml` services other than the new ones, `deploy/k8s`, `.github`, `apps/`, `services/`.
- Docs in English with the Vietnamese twin (`docs/guide.md`, `docs/vi-guide.md`).
- Shared repository: stage only your own paths (`git add <paths>`, never `-A`, `-a`, `stash`, `reset --hard`); never push. Use throwaway project names or the existing `grab-mini` stack deliberately: the user's stack may be running; check `docker ps --format '{{.Names}}' | grep grab-mini` first and say which you used.

## Review Focus

Failure modes the spec implies and no task's happy path exercises; each has a check in the owning task.

1. **Idle partitions stall the windows.** `order.created` and `driver.location.updated` have 3 partitions; with traffic on one of them the watermark never advances, so no window closes and the sink stays empty. `table.exec.source.idle-timeout` must be set, and a check sends orders that land on one partition only (Task 2).
2. **Submitting twice duplicates jobs.** `INSERT INTO` has no `IF NOT EXISTS`; `make flink-up` run twice, or `flink-sql-init` re-run, would double every count. The init must skip a job that is already RUNNING, found by name (Task 2).
3. **`infra-down` and `infra-reset` leave Flink behind.** `docker compose down` only knows services of active profiles, and `down -v` cannot remove `flink-data` while a Flink container uses it (Task 1).
4. **Default stack changes.** A Flink service without the profile, or a `build:` that runs on plain `docker compose up`, breaks `make dev` for anyone offline (Task 1, re-checked in Task 4).
5. **Fresh stack, no data, no schema yet.** The jobs start before the first record: the topics must exist (`kafka-init` creates them) and the jobs must not fail because no Avro record or schema subject has been read yet (Task 2).
6. **Counts differ from ksqlDB** for a legitimate reason (window alignment, record time versus `occurredAt`, late data). Both engines use the Kafka record timestamp, earliest offsets and one-minute tumbling windows so the comparison is like for like; any residual difference is explained in the guide (Tasks 2 and 4).

---

### Task 1: Flink cluster behind a compose profile

**Files:**
- Create: `deploy/flink/Dockerfile`
- Modify: `docker-compose.yml` (services `flink-jobmanager`, `flink-taskmanager`; volume `flink-data`), `Makefile` (`flink-up`, `flink-down`, `flink-sql`, `.PHONY`, `infra-down`, `infra-reset`)

**Interfaces:**
- Produces: image `grab-mini/flink:1.20`; REST on `http://localhost:8082`; network address `flink-jobmanager:8081`; checkpoint directory `/opt/flink/checkpoints` (the `flink-data` volume); `make flink-up` (starts the profile and builds the image), `make flink-down` (stops it, keeps data), `make flink-sql` (an interactive Flink SQL client against the cluster). Task 2 adds the init container on top of this.

- [ ] **Step 1: Define the checks (they fail now).**
  - `docker compose config --services` lists no `flink-*`; `docker compose --profile flink config --services` lists `flink-jobmanager` and `flink-taskmanager`.
  - After `make flink-up`: `curl -s localhost:8082/overview` reports one TaskManager and its slots.
  - `make infra-down` leaves no `grab-mini-flink-*` container; `make infra-reset` leaves no `grab-mini_flink-data` volume.
- [ ] **Step 2: Run the first check.** Expected: FAIL (no such services).
- [ ] **Step 3: Implement.**
  - `deploy/flink/Dockerfile`: `FROM flink:1.20.x` (pin the patch tag that exists on Docker Hub; check it) and add the two connector jars under `/opt/flink/lib` with pinned download URLs whose versions match the Flink minor (verify both exist and match before pinning; say which source you used).
  - `docker-compose.yml`: both services under `profiles: ["flink"]`, `build:` (context `deploy/flink`, `image: grab-mini/flink:1.20`), configuration through `FLINK_PROPERTIES` (`jobmanager.rpc.address: flink-jobmanager`, `taskmanager.numberOfTaskSlots: 2`, `state.checkpoints.dir: file:///opt/flink/checkpoints`, `execution.checkpointing.interval: 10s`, restart strategy fixed-delay with a short delay and a high attempt count, memory sizes that keep the pair near 1.8 GB or less, tuned by measurement), UI `127.0.0.1:8082:8081`, `depends_on` Kafka and Schema Registry healthy, a REST healthcheck, `flink-data` mounted on both. Add `flink-data` to the top-level `volumes`.
  - `Makefile`: `flink-up` runs `docker compose --profile flink up -d --build flink-jobmanager flink-taskmanager`; `flink-down` runs `docker compose --profile flink stop` then `rm -f` for the Flink services; `flink-sql` runs the SQL client in the JobManager container against the cluster; `infra-down` and `infra-reset` become `docker compose --profile flink down` and `... down -v`; each new target has a `##` help line in the Infrastructure section and is listed in `.PHONY`.
- [ ] **Step 4: Run all checks.** Expected: all pass; also `docker compose up -d --dry-run` (or `docker compose config`) without the profile shows no Flink service and no build.
- [ ] **Step 5: Measure** `docker stats --no-stream` for the two Flink containers and record the memory in the report; if the pair exceeds 2 GB, lower the process sizes.
- [ ] **Step 6: Commit** `git add deploy/flink docker-compose.yml Makefile && git commit -m "flink: add a Flink cluster behind a compose profile"`.

### Task 2: Sources and job 1, the orders-per-minute twin

**Files:**
- Create: `flink/sql/01-sources.sql`, `flink/sql/02-orders-per-minute.sql`, `deploy/flink/submit.sh`
- Modify: `docker-compose.yml` (service `flink-sql-init`, profile `flink`), `Makefile` (`flink-up` also waits for the init to finish)

**Interfaces:**
- Consumes: Task 1's cluster at `flink-jobmanager:8081`.
- Produces: tables `orders_created` and `driver_locations` (sources, Avro-Confluent, Kafka record timestamp as event time with a 5 s watermark, `scan.startup.mode = earliest-offset`); job named `flink-orders-per-minute` writing `flink.orders_per_minute` with key (`window_start`, `vehicle_type`) and values `window_start`, `window_end`, `vehicle_type`, `orders`; `deploy/flink/submit.sh <sql-file>...` which submits each file's statements unless a RUNNING job with that file's pipeline name exists. Task 3 reuses `submit.sh` and the sources.

- [ ] **Step 1: Write the checks (they fail now).** With the stack and the profile up and `make loadtest RATE=10 DURATION=30s DRIVERS=10` (or a few `curl` order bookings, like the guide's) run against it:
  - `curl -s localhost:8082/jobs/overview` lists `flink-orders-per-minute` as RUNNING exactly once, also after running `make flink-up` a second time.
  - For a finished minute, the per-vehicle counts read from the `flink.orders_per_minute` topic (Kafka UI, or `kafka-console-consumer.sh` in the Kafka container) equal ksqlDB's `SELECT * FROM orders_per_minute;` for the same window start.
  - A run of orders that all land on one partition (give them the same key by booking for one rider, or read the partition column in Kafka UI) still produces output within about the idle timeout after the window ends.
- [ ] **Step 2: Run them.** Expected: FAIL (no job, no topic).
- [ ] **Step 3: Implement `flink/sql/01-sources.sql`** declaring the two source tables with only the columns the jobs need (take names and types from `services/pkg/events/schemas/order.created.avsc` and `driver.location.updated.avsc`; `vehicleType` defaults to `CAR` in the schema, so `COALESCE(vehicleType, 'CAR')` as in `ksql/statements.sql`), `'format' = 'avro-confluent'`, `'avro-confluent.url' = 'http://schema-registry:8081'`, the record timestamp as the event-time column with `WATERMARK ... INTERVAL '5' SECOND`, and the session settings `SET 'table.exec.source.idle-timeout' = '5 s'` and `SET 'pipeline.name'` per job. Implement `02-orders-per-minute.sql` as a one-minute tumbling aggregation into an `upsert-kafka` sink with JSON key and value. Implement `deploy/flink/submit.sh` (POSIX shell) that waits for the REST API, asks `/jobs/overview` whether a RUNNING job with the pipeline name exists, and only then runs the SQL client with `-f`. Add the `flink-sql-init` service (one-shot, `restart: "no"`, runs `submit.sh` on `01` plus `02`, fails visibly on an error like `ksql-init` does) and make `flink-up` wait for it.
- [ ] **Step 4: Run the checks.** Expected: PASS. Record in the report the exact connector options used and any difference found against ksqlDB with its cause.
- [ ] **Step 5: Fresh-stack check.** `make infra-reset`, `make dev`-style start (or `docker compose up -d`) then `make flink-up` before any traffic: the init exits 0 and the job is RUNNING with an empty sink; then book an order and see it counted.
- [ ] **Step 6: Commit** `git add flink deploy/flink docker-compose.yml Makefile && git commit -m "flink: add the sources and the orders-per-minute job"`.

### Task 3: Job 2, driver sessions, and the checkpoint exercise

**Files:**
- Create: `flink/sql/03-driver-sessions.sql`
- Modify: `docker-compose.yml` (`flink-sql-init` also submits `03`), `deploy/flink/submit.sh` only if Task 2's interface needs it

**Interfaces:**
- Consumes: Task 2's `driver_locations` source and `submit.sh`.
- Produces: job `flink-driver-sessions` writing the append-only `flink.driver_sessions` topic with values `driver_id`, `session_start`, `session_end`, `points` (a session closes after 30 s without a report from that driver).

- [ ] **Step 1: Write the checks (they fail now).**
  - With the simulator running (`make dev`), `flink.driver_sessions` receives rows; stopping `driver-app-sim` for over 30 s plus the watermark and idle timeouts closes the open sessions.
  - **Resume:** `docker kill grab-mini-flink-taskmanager`; both jobs leave RUNNING; `docker start grab-mini-flink-taskmanager` (or compose up); both return to RUNNING and the counts in `flink.orders_per_minute` for a window that was open keep growing from the pre-kill value instead of restarting from zero (record the numbers). The JobManager log or UI shows a restore from a checkpoint.
  - Re-running `make flink-up` does not start a second `flink-driver-sessions`.
- [ ] **Step 2: Run them.** Expected: FAIL (no job).
- [ ] **Step 3: Implement `03-driver-sessions.sql`:** a session window with a 30 s gap per `driverId` on the same event time and watermark as the sources, counting points, into a plain `kafka` sink (JSON). Use the windowing syntax Flink 1.20 supports (check whether the session window table function exists in this version; if not, use the legacy group-window form) and say which.
- [ ] **Step 4: Run the checks.** Expected: PASS. If the resume check shows counts restarting at zero, fix the checkpoint settings from Task 1 before going on; do not weaken the check.
- [ ] **Step 5: Commit** `git add flink docker-compose.yml deploy/flink && git commit -m "flink: add the driver sessions job"`.

### Task 4: Guide section and the default-stack regression

**Files:**
- Modify: `docs/guide.md`, `docs/vi-guide.md`, `Makefile` (help text only, if Task 1's lines need polish)

**Interfaces:**
- Consumes: the targets, ports, topics and job names from Tasks 1 to 3 (use them exactly).

- [ ] **Step 1: Write the section** "Stream processing with Flink (demo)" in `docs/guide.md` (after "Analytics with ksqlDB") and its Vietnamese twin in `docs/vi-guide.md`: what it is (a learning demo beside ksqlDB, not part of the product, output topics are not event contracts), `make flink-up` and the UI at http://localhost:8082, the two jobs, how to read the output topics in Kafka UI, how to compare with ksqlDB (the exact queries), and three things to try: compare the minute counts; stop `driver-app-sim` and watch sessions close; the TaskManager kill and restore exercise with the real numbers' shape. State the known differences (record time, window alignment, late data) found in Task 2, and the internet need at first build. Add the three make targets to the Makefile list of each guide and the new ports to the port note. Keep the English and Vietnamese commands identical.
- [ ] **Step 2: Regression checks (the point of this task).**
  - With the profile **not** started: `docker compose config --services` has no `flink-*`; `make dev` still reaches "grab-mini is up" and `ksql-init` still passes (or, if the user's stack must not be disturbed, `docker compose up --dry-run` and `docker compose config` show no change in the default service set; say which you did).
  - `make flink-up`, then `make infra-down`: no `grab-mini-flink-*` container remains; `make infra-reset`: no `grab-mini_flink-data` volume remains.
  - `git status` shows only the intended paths; no change under `ksql/`, `deploy/k8s`, `.github`, `apps/`, `services/`.
- [ ] **Step 3: Commit** `git add docs/guide.md docs/vi-guide.md Makefile && git commit -m "docs: add the Flink demo to the guides"`.
- [ ] **Step 4: Cleanup.** Remove any throwaway container, volume or image you created for testing and list what you left running.

---

## Known, not fixed (report, do not change)

- A JobManager restart loses the submitted jobs (no HA); resubmit with `make flink-up`. Restoring from the checkpoint after a JobManager loss is manual and out of scope.
- The demo has no security (no auth on the Flink UI), single TaskManager, replication factor 1: local only.
- The JSON sink topics are not in `kafka-init` and not under any ACL; they are created by the sink.
- Flink 1.20 is the version this plan targets; moving to Flink 2.x is a separate change.

## Could not verify while planning

- That the pinned Flink patch tag and the two connector jars exist and match each other (Task 1 verifies and records the source).
- The exact SQL client flags for submitting to a remote session cluster in application of `-f` (Task 2 verifies).
- Whether the session-window table function exists in 1.20 (Task 3 verifies and says which syntax it used).
- Memory use of the pair (Task 1 measures).
- Whether the user's own stack is running during execution; tasks check before touching it.
