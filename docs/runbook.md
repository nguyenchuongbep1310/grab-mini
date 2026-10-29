# Runbook

What breaks, where to look, what to do. Each section below corresponds to an alert in
[deploy/monitoring/alerts.rules.yaml](../deploy/monitoring/alerts.rules.yaml): the alert's `runbook`
field points to the section heading (do not rename a heading without updating the alert).

> **Scope of verification.** The project's tools (`dlq-redrive`, Postgres queries, Redis keys,
> metrics, logs) have been tried in a local test environment. The `kubectl` commands and the Strimzi/KEDA
> resources have **not been run on a real cluster**: the manifests have passed schema checks and cross-checks
> (`deploy/verify.sh`) but have not been applied. Try them in staging the first time you use them.

## How to investigate an incident

1. **What does the alert say?** The alert name tells you which system is affected. `severity: page` means riders
   are affected or data is at risk right now.
2. **Which pod is unhealthy?** Each service's `/readyz` returns JSON saying which dependency is failing
   (admin port `9090`, not the API port):
   ```bash
   kubectl -n grab-mini port-forward deploy/trip-service 9090 &
   curl -s localhost:9090/readyz    # {"checks":{"kafka":"ok","postgres":"ok","redis":"connection refused"},"status":"unavailable"}
   ```
3. **Where did a specific order go?** Logs are JSON, and every line has `service`; lines inside a span also have
   `trace_id`. Search by `orderId`, or by `trace_id` to see what every service did for the same request
   (HTTP, outbox, each Kafka hop, and all retries are in one trace).
4. **Consumer state:**
   ```bash
   kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP --command-config ops.properties --describe --group matching-service-group
   ```
   (`ops.properties` holds the operator's credentials; see `deploy/kafka-secure` and the Strimzi `KafkaUser/ops`.)

Dashboards worth having: lag per group, `kafka_consumer_messages_total` by `result`,
`outbox_oldest_pending_age_seconds`, `matching_latency_seconds`, `http_requests_total` by route and code.

---

## Consumer lag

**Alerts:** `KafkaConsumerLagGrowing`, `KafkaLocationConsumerLagGrowing`.
**Meaning:** a consumer group is processing slower than messages arrive. Riders see slow or missing status updates.

Lag on `*.retry.N` groups is normal (messages waiting until they are due); the alerts exclude them.

**Check in this order:**

1. Is the lag really growing, or was there just a spike? Look at `kafka_consumergroup_lag` by `partition`:
   - **One partition lagging, the others not:** a "heavy" message or a hot key. Look in the logs for the
     `topic`, `partition` and `offset` of the message being processed.
   - **All partitions lagging:** the consumers lack capacity, or a dependency is slow.
2. Is the handler slow? `histogram_quantile(0.95, sum by (le, group) (rate(kafka_consumer_handle_duration_seconds_bucket[5m])))`.
   A sudden increase means Postgres or Redis is slow (see `/readyz`, and the [Pods not ready](#pods-not-ready) section).
3. Is there a retry storm? `rate(kafka_consumer_messages_total{result="retried"}[5m])`. If so, the cause
   is a failing dependency: see [Dead letters](#dead-letters).
4. Is KEDA scaling? `kubectl -n grab-mini get scaledobject`, then `describe` the relevant one.
   The replica count is **capped by the partition count** (6, 12 or 48 depending on the topic): adding replicas beyond the partition count
   does not help. If replicas are already at the maximum and there is still lag, you need to increase the partition count (it can only be increased, and it changes
   the key → partition mapping, so ordering may be temporarily off during the change) or make the handler faster.
5. Constant rebalancing? The log shows repeated `consumer stopped` / `consumer running`, or pods are restarting: check
   `kubectl get pods` (RESTARTS) and `kubectl describe pod` (OOMKilled?).

**Do not:** reset offsets to "skip" the lag. A skipped message is an event that is never processed
(an order that is never matched, a rating that is never counted).

## Dead letters

**Alerts:** `KafkaMessagesDeadLettered` (page, any message), `KafkaConsumerFailureRatioHigh`.
**Meaning:** a message could not be processed after all retries, so it was parked in `<topic>.DLQ`. That event
**has not been processed**.

1. **See what it is.** Each dead letter is JSON: `originalTopic`, `error`, `retries`, `valueText` (if it is text)
   and `valueBase64` (the original bytes). Read it as the operator:
   ```bash
   kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP --consumer.config ops.properties \
     --topic order.created.DLQ --from-beginning --max-messages 5
   ```
   Or preview without changing anything: `dlq-redrive -topic order.created -dry-run`.
2. **Classify by `error` and `retries`:**
   | Sign | Common cause | What to do |
   |---|---|---|
   | `retries: 0`, error `not a Schema Registry Avro message` or `isn't readable with the ... schema` | Someone wrote a message in the wrong format, or made an incompatible schema change | Fix the writer. For a genuinely corrupt message, **do not redrive**: it will land in the DLQ again |
   | `retries: 3` (all tiers), `postgres`/`redis` connection error | A dependency was down for a while | Wait for the dependency to recover, then redrive |
   | Business error repeating for the same message | A bug in the handler | Fix the code, deploy, then redrive |
3. **Redrive** after the cause has been fixed. Redrive pushes messages back to the original topic with the **exact key and bytes**; the consumer
   processes them again (consumers are idempotent, so processing twice is safe):
   ```bash
   # see what it would do
   dlq-redrive -topic order.created -dry-run
   # a single cause only, at most 100 messages at a time
   dlq-redrive -topic order.created -error "connection refused" -max 100
   # everything
   dlq-redrive -topic order.created
   ```
   It needs the operator's credentials (the services' principals **cannot** read the DLQ, by design), configured with
   the same `KAFKA_*` variables as every service (`KAFKA_BROKERS`, `KAFKA_SECURITY_PROTOCOL`, `KAFKA_USERNAME`,
   `KAFKA_PASSWORD_FILE`, `KAFKA_CA_FILE`). On Kubernetes, run it as a Job using the `dlq-redrive` image with the `ops` user's secret.
   - Running it again sends only what is new: the position is remembered in the consumer group `dlq-redrive-<topic>`
     (each `-error` filter has its own group, because a group cannot skip messages).
   - A message that fails again lands in the DLQ once more as a new dead letter.
4. **Verify afterwards:** `kafka_consumer_messages_total{result="dead_lettered"}` stops increasing and lag returns to 0.

**`driver.location.updated` specifically:** dead letters here do not page (a stale location is worthless).
Do not redrive: a fresh location arrives within seconds, and a redrive would be discarded anyway because of the old `timestamp`.

## Kafka broker down

**Alerts:** `KafkaBrokersMissing`, `KafkaUnderReplicatedPartitions`, `KafkaPublishErrors`, `KafkaPublishSlow`.

Setup: 3 brokers, one per zone, `replication.factor=3`, `min.insync.replicas=2`
(location topic: `1`), unclean leader election disabled.

- **Losing 1 broker** (or 1 zone): the system **can still write** (2 in-sync replicas remain), but there is no redundancy left. Losing one
  more means producers get `NotEnoughReplicas`. Treat this as an incident to handle immediately even if users have not noticed anything.
- **Losing 2 brokers:** the business topics cannot be written.
  - Order Service and Trip Service: events sit in the `outbox` table (committed together with the state change) and are **not lost**.
    `outbox_oldest_pending_age_seconds` grows; see [Outbox backlog](#outbox-backlog). When the brokers return, the relay catches up and sends them.
  - Matching Service and Location Gateway publish directly, **with no outbox**. The gateway returns 503 and the driver resends the
    location a few seconds later. Matching: if `order.matched` cannot be published, the handler returns an error and releases the driver (`ReleaseMatch`),
    and the message goes down the retry path. If neither the retry topic nor the DLQ can be written either, the consumer **holds on to the partition**
    (log `cannot put a failed message on a retry or dead-letter topic`, lag grows) rather than committing and losing the message,
    and resumes once Kafka is writable again.

**Check:**
```bash
kubectl -n grab-mini get kafka,kafkanodepool,pods -l strimzi.io/cluster=grab-mini
kubectl -n grab-mini describe kafka grab-mini           # Ready condition, reason
kubectl -n grab-mini logs grab-mini-broker-1            # example broker pod
```
`kafka_topic_partition_under_replicated_partition` tells you which topic and partition; `kafka_brokers` gives the broker count.

**Fix:** brokers usually come back on their own (pod restart, node move). If not:
1. Is the pod `Pending`? Usually insufficient resources or the PVC cannot attach in that zone
   (`kubectl describe pod`): a broker must run in the zone where its disk is.
2. Disk full? `kubectl exec ... -- df -h /var/lib/kafka`. Lower `retention.ms` on the largest topic (usually
   `driver.location.updated`) or expand the PVC.
3. **Do not** enable `unclean.leader.election.enable` to "get it running again": it trades acknowledged data for availability.

**After recovery:** URP returns to 0, `outbox_oldest_pending_age_seconds` returns to 0, lag falls. No redrive is needed: no
message is dead-lettered merely because a broker was briefly down (they are retried).

## Outbox backlog

**Alerts:** `OutboxBacklog` (an event waiting more than 30 seconds), `OutboxEventQuarantined`.
**Meaning:** state has been saved but **not announced**: riders and other services do not know about it yet.

1. **Look at the table** (the Order or Trip Postgres):
   ```sql
   SELECT source, count(*) AS waiting, min(created_at) AS oldest FROM outbox GROUP BY source;
   SELECT id, source, topic, failed_at, left(error, 120) FROM outbox_dead ORDER BY failed_at DESC LIMIT 20;
   ```
2. **Is the relay running?** In the order-service/trip-service logs, search for `[outbox:...] publish failed`: the reason is right there.
   - `connection refused` / `broker not available`: see [Kafka broker down](#kafka-broker-down).
   - `Topic Authorization Failed`: the service's ACL was changed or is missing (its own `KafkaUser` needs `Write` on the topic).
     Fix the ACL; the relay will catch up and send the events by itself, nothing more to do.
   - `Schema Registry`: unrelated to the relay (the payload was already Avro-encoded at request time); this error shows up at order creation.
3. **Is the relay stuck on a lock?** Only one relay runs per source at a time (advisory lock). If a hung pod holds
   an open transaction, the other pods skip:
   ```sql
   SELECT pid, state, now() - xact_start AS open_for, left(query, 80) FROM pg_stat_activity WHERE state <> 'idle' ORDER BY xact_start;
   ```
   A relay transaction open for a very long time: restart that pod.
4. **`outbox_dead`** (alert `OutboxEventQuarantined`): the relay has **set aside** an event that Kafka permanently rejects
   (usually too large for `message.max.bytes`, or an invalid topic), so that the events behind it are not blocked.
   That event **will not be sent**: the state is saved but no one is notified.
   - Read `error`, `topic` and the size (`length(payload)`). The payload is binary Avro, so it cannot be read as text.
   - Fix the cause (usually a bug that produces an oversized payload).
   - To resend after the fix: copy the row back into `outbox`, then delete it from `outbox_dead`:
     ```sql
     BEGIN;
     INSERT INTO outbox (source, topic, key, payload, headers, created_at)
       SELECT source, topic, key, payload, headers, now() FROM outbox_dead WHERE id = <id>;
     DELETE FROM outbox_dead WHERE id = <id>;
     COMMIT;
     ```
     Only do this when Kafka will accept it now. Otherwise, make a business decision: reconcile the state by hand.

## Slow matching

**Alerts:** `MatchLatencySLOBreach` (p95 above 3 seconds), `NoDriverRatioHigh` (more than 30% of orders have no driver).

Match time = from when `order.created` is produced to when `order.matched` is published
(`matching_latency_seconds`). When no driver is available, Matching retries `MATCH_RETRIES` (default 3) times, `MATCH_RETRY_DELAY_MS`
(default 2000 ms) apart: an order for which **no driver is found** takes about 6 seconds before reporting
`order.no_driver_found`, and that time counts toward the latency. That is why the two alerts often fire together.

1. **Lag of `matching-service-group`** on `order.created`: see [Consumer lag](#consumer-lag). Orders are waiting in the topic.
2. **Redis slow or down:** the matching-service `/readyz` (admin port). Matching depends on Redis for `drivers:geo`
   and driver state.
3. **A real driver shortage, or bad data?** Is `rate(location_updates_total{result="applied"}[5m])` steady?
   If locations are not being updated, Driver Location Service or Location Gateway is broken
   (`location_reports_total` by `result`: `published`, `stationary`, `too_fast`, `invalid`, `error`).
4. **Driver stuck in a state.** Matching only accepts drivers that are `AVAILABLE` (or `ASSIGNED` with an expired lease, default 120 seconds).
   A driver that is `ON_TRIP` with no running trip is stuck. Check both sources:
   ```sql
   -- Trip Service Postgres: does the driver have a running trip?
   SELECT driver_id, id, status FROM trips WHERE driver_id = 'driver-7' AND status IN ('ASSIGNED','IN_PROGRESS');
   ```
   ```bash
   redis-cli GET driver:driver-7:status      # AVAILABLE | ASSIGNED | ON_TRIP
   redis-cli GET driver:driver-7:assigned_until
   ```
   If there is **no running trip** but Redis still says `ON_TRIP`, the `trip-driver-release-group` consumer is behind or broken
   (its lag); once that is fixed, it releases the driver by itself. Release by hand only when you are sure no trip remains:
   `redis-cli SET driver:driver-7:status AVAILABLE`, then `redis-cli DEL driver:driver-7:assigned_until`.
5. **Stale locations:** `drivers:geo` keeps each driver's latest location; a driver who stops sending locations stays in the GEO set with the
   old location. There is no TTL: an offline driver can still be matched. (This is a known limitation, see production-design.)

## Booking errors

**Alerts:** `OrderCreationErrorBudgetBurn` (page), `HTTPServerErrorsHigh`.
Target: 99.9% of `POST /orders` succeed. The alert fires when the error budget is burning 14 times too fast, over both the 5-minute and the 1-hour window.

`POST /orders` does: token validation, Avro encoding (needs the **Schema Registry**), a Postgres write (order + outbox in one transaction).
A 5xx means one of those three is broken. Tell them apart using the order-service logs (`failed to encode order.created`
→ Schema Registry; `failed to save order` → Postgres) or `/readyz` (Postgres, Kafka).

- **Schema Registry down:** each service remembers its schema ids after the first registration, so **running** pods keep accepting orders
  normally. The 500 errors show up on **newly started** pods (which could not register) and on consumers that meet a `schema id` they have never
  seen (a new schema version). So a registry problem usually shows up after a deploy or scale-out, not right
  when the registry dies. Restore the registry, then check `/readyz`.
- **Postgres down:** see [Pods not ready](#pods-not-ready); Postgres failover is the provider's/DBA's job.
- **A sudden rise in 401s** (not 5xx, but users see errors): `JWT_JWKS_URL` is unreachable (look for `auth: refreshing signing keys`
  in the logs, and check the egress NetworkPolicy allows the issuer), the issuer just rotated its key (services fetch the new key when they
  meet an unknown `kid`, at most once every 15 seconds), or tokens expired en masse. See [security.md](security.md).

## Surge pricing

**Alerts:** `SurgePricingFailOpen` (the surge multiplier could not be read, price falls back to 1.0), `SurgeSamplerStalled` (pricing-service has stopped sampling supply/demand).

Surge **never blocks booking**: any Redis read error yields a multiplier of 1.0, so the consequence of an incident is *prices not going up*, not bookings failing.

1. **Look at the current multiplier of a cell:** cell = `floor(lat/degrees):floor(lng/degrees)` (degrees = `SURGE_CELL_DEGREES`, or `GEO_SHARD_DEGREES`, default 0.05).
   ```bash
   redis-cli SMEMBERS surge:cells                      # cells with demand, in the form CAR|215:2134
   redis-cli GET surge:mult:CAR:215:2134               # the multiplier; no key = 1.0
   redis-cli TTL surge:mult:CAR:215:2134               # at most 30 seconds, refreshed every 5 seconds
   redis-cli MGET surge:demand:CAR:215:2134:<unix minute/60>
   ```
2. **`SurgePricingFailOpen`:** Redis is slow/down (300 ms read timeout). See [Pods not ready](#pods-not-ready), and the `/readyz` of pricing-service and order-service.
3. **`SurgeSamplerStalled`:** `pricing_ticks_total` is not increasing. Only one instance holds the `surge:sampler:lock` lock each cycle; check that the
   pricing-service pods are running, the `surge sample failed` log, and `pricing_sample_errors_total` (`driverlocation.Search` errors, for example a radius exceeding `maxCells` when `GEO_SHARD_DEGREES` is too small).
   A lagging `pricing-service-group` consumer makes *demand* undercounted: see [Consumer lag](#consumer-lag).
4. **Riders report a 409 when booking:** by design. The quote has expired and the multiplier has drifted by more than `SURGE_QUOTE_TOLERANCE` (0.2); the app shows the new price for confirmation.
   Many **400 "quote is invalid"** errors mean `QUOTE_SECRET` differs between order-service and pricing-service (or someone is forging quotes).
5. **Abnormally high prices:** `pricing_surge_multiplier_max` / `pricing_surging_cells`; the cap is `SURGE_MAX` (3.0). To turn surge off in an emergency:
   set `SURGE_MAX=1` for pricing-service and restart, or `redis-cli DEL` the `surge:mult:*` keys (they are recreated at the next sampling if the configuration has not changed).
6. **Old data:** orders created before migration 00006 have `price` NULL and `surge_multiplier` 1.0. Old `order.created` messages (schema v2) are read with `surgeMultiplier` 1.0.

## Redis failover

**Cảnh báo:** `RedisNotExactlyOneMaster` (0 master quá 1 phút: failover không xong; 2 master: split brain), `RedisMemoryNearLimit`.

Redis chạy như StatefulSet `redis` (3 pod, mỗi pod: `redis` + `sentinel` + `exporter`, xem [disaster-recovery.md](disaster-recovery.md#0-redis-sentinel-mất-một-node-không-phải-mất-vùng)).
Service tìm master qua Sentinel (`REDIS_SENTINEL_ADDRS`), nên failover **không cần** restart service; chỉ có vài giây lỗi mà consumer retry.

1. **Ai là master?** (mật khẩu ở Secret `grab-mini-redis`, key `password`)
   ```bash
   P=$(kubectl -n grab-mini get secret grab-mini-redis -o jsonpath='{.data.password}' | base64 -d)
   for i in 0 1 2; do echo -n "redis-$i: "; kubectl -n grab-mini exec redis-$i -c redis -- env REDISCLI_AUTH="$P" redis-cli role | head -1; done
   kubectl -n grab-mini exec redis-0 -c sentinel -- env REDISCLI_AUTH="$P" redis-cli -p 26379 sentinel get-master-addr-by-name mymaster
   kubectl -n grab-mini exec redis-0 -c sentinel -- env REDISCLI_AUTH="$P" redis-cli -p 26379 sentinel ckquorum mymaster
   ```
   `ckquorum` phải trả `OK`. Nếu không, đa số Sentinel đang mất: xem pod nào `NotReady` (`kubectl get pods -l app.kubernetes.io/name=redis -o wide`),
   và pod có nằm trên cùng một node/zone không (anti-affinity bắt buộc khác node, topologySpread bắt buộc khác zone).
2. **0 master:** Sentinel cần `down-after-milliseconds` (5 s) rồi bầu chọn. Quá một phút: `kubectl logs redis-0 -c sentinel` (`+sdown`, `+odown`, `-failover-abort-*`).
   Thường là mất quorum (2/3 Sentinel). Khôi phục pod bị mất; **không** tự `SLAVEOF NO ONE` trên một node trừ khi chắc chắn cả ba Sentinel đã mất.
3. **2 master (split brain):** dữ liệu ghi vào master cũ **sẽ mất** khi nó bị hạ xuống. Tìm master mà Sentinel công nhận (bước 1), `kubectl delete pod` node còn lại:
   khi khởi động lại, `redis-start.sh` hỏi Sentinel và vào làm replica. Vị trí tài xế tự bù sau vài giây; lease/`match:<orderId>` ghi trong cửa sổ split brain có thể mất.
4. **Failover có kế hoạch:** `kubectl drain` hoặc rollout chạy `preStop` (`redis-prestop.sh`): master xin Sentinel chuyển vai trò rồi mới tắt (đo được ~12 s). PDB `redis` chỉ cho mất 1 pod một lúc.
5. **Bộ nhớ gần đầy:** `maxmemory` 400 MB, chính sách `noeviction`: đầy thì **ghi lỗi** (không âm thầm xóa lease). Nguyên nhân thường gặp: key không TTL
   (xem Slow matching, "Không có TTL cho vị trí"). `redis-cli --bigkeys`, `INFO memory`. Tăng `maxmemory` và `limits.memory` trong `deploy/k8s/generate.py` (giữ `limits` ≥ 1.8x `maxmemory` cho AOF rewrite).
6. **Pod Redis không lên sau khi mất PVC / hỏng AOF:** dữ liệu Redis không quan trọng (tự hồi phục): xóa PVC của pod đó
   (`kubectl delete pvc data-redis-N` rồi xóa pod) để nó đồng bộ lại từ master. **Không** làm vậy với master hiện tại.
7. **Xoay vòng mật khẩu Redis:** đổi Secret `grab-mini-redis`, rồi restart StatefulSet (từng pod, `kubectl rollout restart sts/redis`) **và** mọi service dùng Redis
   (mật khẩu chỉ được đọc lúc khởi động). Có một khoảng service không kết nối được; làm trong giờ thấp điểm.

Dev (`docker-compose.yml`) vẫn là một Redis không mật khẩu, `REDIS_SENTINEL_ADDRS` trống.

## Backup and restore

**Cảnh báo:** `PostgresBackupStale` (không có bản sao lưu thành công 24 giờ; job chạy mỗi 6 giờ), `DataOpsJobFailed` (job `backup-*` hoặc `retention-*` lỗi).

Bối cảnh: CronJob `backup-<service>` (6 giờ một lần) chạy `pg_dump -Fc` rồi đẩy lên `BACKUP_TARGET` (ConfigMap `grab-mini-data-ops`);
`retention-order-orders`, `retention-trip-trips` (hằng ngày 03:43) xóa đơn/chuyến đã kết thúc quá `RETENTION_DAYS` (180). Số liệu RPO/RTO: [disaster-recovery.md](disaster-recovery.md#1-postgres).

1. **Job lỗi:** `kubectl -n grab-mini get jobs | grep -E 'backup|retention'`, `kubectl -n grab-mini logs job/<name> -c dump` (hoặc `-c upload`, `-c retention`).
   - `dump` lỗi: không kết nối được DB (Secret `grab-mini-db-<service>`), hoặc `pg_dump` phiên bản thấp hơn server (image `postgres:16-alpine` trong `generate.py`; tăng cùng server).
   - `upload` lỗi: Secret `grab-mini-backup` (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`), `BACKUP_TARGET` còn là `s3://CHANGE-ME-...`, quyền ghi bucket, hoặc NetworkPolicy chặn egress tới S3.
   - Chạy lại ngay: `kubectl -n grab-mini create job --from=cronjob/backup-order-service backup-order-manual-$(date +%s)`.
2. **Diễn tập khôi phục (mỗi quý, và sau mỗi thay đổi job sao lưu).** Cần một Postgres *scratch* (không phải production) và `psql`, `pg_restore` (phiên bản ≥ server), AWS CLI:
   ```bash
   export BACKUP_TARGET=s3://<bucket>/postgres   # như ConfigMap grab-mini-data-ops
   export SCRATCH_URL=postgres://admin:...@scratch-host:5432/postgres
   for s in order-service trip-service rating-service; do
     BACKUP_NAME=$s scripts/pg-restore-drill.sh || echo "DRILL FAILED: $s"
   done
   ```
   Script lấy bản mới nhất, khôi phục vào DB `restore_drill_*` (tự xóa), kiểm tra: archive đọc được, `pg_restore` không lỗi, `schema_migrations_<service>` ở đúng phiên bản
   mới nhất của code, các bảng của service đều có, bảng chính có ≥ `MIN_ROWS` dòng; in số dòng và thời gian. Đặt `SOURCE_URL` để in số dòng của DB sống bên cạnh.
   **Ghi lại thời gian và kích thước vào ticket diễn tập**: đó là số RTO thật duy nhất bạn có.
3. **Khôi phục thật** (mất dữ liệu): (a) dừng ghi: scale service về 0 (hoặc tạm dừng KEDA); (b) tạo DB mới, `pg_restore --no-owner --dbname=<url> <file>` từ bản sao lưu mới nhất
   (hoặc PITR của dịch vụ managed, ưu tiên hơn vì mất ít dữ liệu hơn); (c) nếu bản sao lưu cũ hơn migration hiện tại, chạy Job `<service>-migrate`;
   (d) đổi Secret `grab-mini-db-<service>` trỏ DB mới, bật lại service; (e) đối chiếu như bước 9 của quy trình chuyển vùng.
   Dữ liệu từ lúc dump tới lúc hỏng **mất** (trừ những gì còn trong Kafka 7 ngày và replay được: xem "Replay một topic", nhưng consumer idempotent chỉ giúp với event đã commit vào outbox rồi).
4. **Xem trước retention / xóa nhầm:** CronJob không có chế độ dry-run; từ máy có `psql`, chạy
   `DRY_RUN=1 DATABASE_URL=... sh scripts/pg-retention.sh orders` (hoặc `trips`) để đếm số dòng sẽ bị xóa. Dòng đã xóa chỉ lấy lại được từ bản sao lưu
   (khôi phục vào DB scratch như bước 2 rồi `COPY` ra). Muốn giữ lâu hơn: tăng `HISTORY_RETENTION_DAYS` trong `deploy/k8s/generate.py` (không dưới 30; script từ chối).
5. **Dọn bản sao lưu cũ** là lifecycle rule của bucket (ví dụ 35 ngày), không phải của job. Cấp cho credential của job quyền ghi mà không có quyền xóa:
   job bị xâm phạm thì không xóa được các bản sao lưu.

## Payments

**Cảnh báo:** `PaymentConsumerLagHigh` (ticket), `PaymentDeadLettered` (page), `LedgerUnbalancedTransactionRejected` (page).
**Nghĩa là:** Payment Service không xử lý kịp, hoặc không xử lý được, một event `order.created` / `trip.status.changed` /
`trip.rated`. Hậu quả: một chuyến **chưa được tính tiền** (cước, tip hoặc phí huỷ), hoặc tài xế chưa được ghi có.
Không có tiền nào bị tính sai: mỗi khoản là một dòng `payments` duy nhất theo `(trip_id, kind)` và một giao dịch sổ cái
cân bằng, nên xử lý lại luôn an toàn.

**Kiểm tra theo thứ tự:**

1. **Lag:** mục [Consumer lag](#consumer-lag), group `payment-service-group` (một group đọc cả ba topic). Số replica bị chặn
   bởi số partition.
2. **Dead letter:** `PaymentDeadLettered` có nhãn `topic`. Xem nội dung bằng `dlq-redrive -topic <topic> -dry-run`.
   Hai nguyên nhân hay gặp:
   - `no price recorded yet for order ...`: `trip.status.changed` đến mà `order.created` chưa được ghi (hai topic khác nhau
     nên không có thứ tự). Consumer đã thử lại qua các tầng retry (5s, 1 phút, 10 phút). Nếu vẫn lỗi thì `order.created`
     thật sự thiếu (đã dead-letter, hoặc đơn tạo trước khi có Payment Service): xem `order.created.DLQ`, redrive nó
     **trước**, rồi redrive `trip.status.changed`.
   - `invalid payment event` / `invalid amount`: event không bao giờ tính được (thiếu driver, tip âm, tiền tệ khác VND).
     Redrive vô ích; sửa nguồn gửi.
3. **Nhà cung cấp thanh toán** lỗi hoặc timeout: `payment_provider_errors_total` tăng, event đi theo đường retry. Khóa
   idempotency (`fare:<tripId>`, `tip:<tripId>`, `cancellation_fee:<tripId>`) làm việc thử lại không tính hai lần.
   Redrive sau khi nhà cung cấp khỏe lại.
4. **`LedgerUnbalancedTransactionRejected`:** code dựng một giao dịch không cộng về 0, và bị từ chối **trước khi ghi**.
   Đó là bug, không phải sự cố dữ liệu. Event liên quan nằm trong DLQ; sửa code, deploy rồi redrive.
5. **Kiểm tra sổ cái bằng SQL** (phải trả về 0 dòng và tổng 0):
   ```sql
   SELECT txn_id, SUM(amount) FROM ledger_entries GROUP BY txn_id HAVING SUM(amount) <> 0;
   SELECT COALESCE(SUM(amount), 0) FROM ledger_entries;
   ```
6. **Một chuyến không có thanh toán?** `SELECT * FROM payments WHERE trip_id = '...'`. Không có dòng nào và không có DLQ:
   kiểm tra `payments_skipped_total{reason}` (đơn không có giá, chuyến miễn phí, tài xế huỷ, `CANCELLATION_FEE_VND=0`).
   Một dòng `DECLINED` nghĩa là nhà cung cấp từ chối thẻ của rider; dòng đó không có bút toán sổ cái và hệ thống **không
   tự thử lại** (xử lý nợ là việc của người vận hành).

**Không nên làm:** sửa hoặc xoá `ledger_*` (có trigger chặn; sổ cái chỉ thêm). Muốn sửa sai, thêm một giao dịch đảo.

## Pods not ready

**Alerts:** `DeploymentReplicasUnavailable`.

1. `kubectl -n grab-mini get pods`, then `kubectl -n grab-mini describe pod <pod>` (events: `Unhealthy`, `OOMKilled`, `ImagePullBackOff`).
2. Pod `Running` but `0/1 Ready`: `/readyz` (admin port) says which dependency is failing; see the top of this document.
   A service is not ready when its **dependency** is failing, and that is the correct behavior: it leaves rotation without being restarted.
   A pod is never restarted because Postgres is down (the `/livez` liveness probe depends on nothing external).
3. Rollout stuck after a deploy: `kubectl rollout status deploy/<name>`; if the new pods are not `Ready`, the old pods are kept
   (`maxUnavailable: 0`), so users are not affected. Use `kubectl rollout undo deploy/<name>` to go back, provided the
   migration that ran is backward compatible (see below).

---

## Common procedures

### Deploy

1. Build and push the images; set the tag: `kustomize edit set image ghcr.io/example/grab-mini/order-service=<registry>/order-service:<tag>` (for each service).
2. **Migrations first**: run the `*-migrate` Jobs and wait for them to finish (`kubectl wait --for=condition=complete job/order-service-migrate`).
   Migrations only go forward, and must be compatible with **both the old and the new version** of the code (add columns with a default; do not drop or rename columns
   while the old version is still running), because a rolling update runs both versions side by side.
3. `kubectl apply -k deploy/k8s/base` (or the environment's overlay). Watch `kubectl rollout status`.
4. Changing the **Avro schema**: only add fields with defaults (the registry runs `BACKWARD`). Register the schema in CI before deploying.

### Replay a topic (reprocess from the beginning)

This is only possible because every consumer is idempotent (developer guide: "Replay a topic and see nothing counted twice"). Pause KEDA first,
otherwise it will scale back up:
```bash
kubectl -n grab-mini annotate scaledobject/rating-service autoscaling.keda.sh/paused-replicas=0 --overwrite
kubectl -n grab-mini scale deploy/rating-service --replicas=0
kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP --command-config ops.properties \
  --group rating-service-group --reset-offsets --to-earliest --topic trip.rated --execute
kubectl -n grab-mini annotate scaledobject/rating-service autoscaling.keda.sh/paused-replicas-   # remove the pause
```
Do not replay `driver.location.updated` (old locations are discarded because of `timestamp`) or any topic with non-idempotent side effects.

### Rotate secrets

- **A service's Kafka password:** delete the Secret Strimzi created for that `KafkaUser` so it is regenerated, then restart the service's pods
  (the password is only read at startup). Do one service at a time.
- **Token signing key (Ed25519):** only the issuer holds the private key. Rotating it does not break live tokens: give the issuer the new
  key and keep the old public key in `JWT_PREVIOUS_PUBLIC_KEYS` until the last token signed with it expires (`TOKEN_TTL` + 10 minutes of cache).
  Services that use JWKS need no restart. Details in [security.md](security.md).
- **Schema Registry password:** change the `grab-mini-schema-registry` Secret, then restart (it is read at startup).
- **Postgres password:** change it in the `grab-mini-db-<service>` Secret, then restart; the service reads `DATABASE_URL` at startup. The `backup-*`/`retention-*`
  CronJobs read the same Secret on every run, so they need nothing.
- **Redis password:** see [Redis failover](#redis-failover), step 7.

### Manual scaling

KEDA controls the replica count; `kubectl scale` is overwritten by KEDA. Change `minReplicaCount`/`maxReplicaCount` in the `ScaledObject`
(via `deploy/k8s/generate.py`), or pause it temporarily with the `autoscaling.keda.sh/paused-replicas` annotation as above.
The useful replica count of a consumer does not exceed the partition count of the topic it reads.

### What is missing (to know when on call)

- **Không có outbox cho Matching và Location Gateway**: nếu Kafka hỏng đúng lúc, một lần ghép có thể không được thông báo
  (đã có lease để tài xế không kẹt, và message `order.created` đi qua retry), còn vị trí tài xế thì tự bù sau vài giây.
- **Không có TTL cho vị trí trong Redis GEO** (xem Slow matching).
- **Notification Service best-effort**: tin gửi khi không ai subscribe thì mất; app khôi phục bằng REST sau khi kết nối lại.
- **Sao lưu và DR**: xem [disaster-recovery.md](disaster-recovery.md). Sao lưu Postgres và diễn tập khôi phục đã chạy trên Postgres thật ở máy dev
  (`scripts/test-data-ops.sh`), nhưng **chưa trên cluster thật**: CronJob chưa được chạy bởi một kubelet, chưa có bản sao lưu nào lên S3 thật, chưa có diễn tập nào với dữ liệu cỡ production.
  Manifest chuyển vùng (MirrorMaker 2, S3 archive) vẫn chưa chạy thử.
