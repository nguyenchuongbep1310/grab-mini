# Backup and disaster recovery

Scope: loss of a region, or loss of data. Losing a single broker or a single zone is already covered by the 3-broker, 3-zone cluster (see the
"Kafka broker down" runbook); this document does not repeat that.

## How much each kind of data matters

| Data | Lives in | If lost | Protection |
|---|---|---|---|
| Orders, trips, ratings, outbox | Postgres (order, trip, rating) | **Truly lost**: it is the source of truth | Postgres backups + PITR (below) |
| Business events (`order.*`, `trip.*`) | Kafka, retained 7 days | Committed data can be rebuilt from Postgres; a consumer more than 7 days behind loses events | MirrorMaker 2 to the standby region; S3 archive |
| Driver location, driver status, leases | Redis (one master + replica, monitored by Sentinel) | **Self-healing**: drivers report again every few seconds; when a lease expires the driver can receive orders again | No backup needed |
| Avro schemas | Schema Registry (Kafka topic `_schemas`) | New producers/consumers cannot decode messages | Lives in Kafka: replicated by MirrorMaker 2 if `_schemas` is added to `topicsPattern`; or re-register from `services/pkg/events/schemas/` (services register themselves on startup) |

RPO/RTO theo từng lớp, tách **đã đo** khỏi **mục tiêu** (chỉnh theo hợp đồng thật):

| Lớp | RPO (mất dữ liệu tối đa) | RTO (thời gian khôi phục) | Trạng thái |
|---|---|---|---|
| Redis mất 1 node | 0 đến ~1 giây ghi cuối (AOF `everysec`, sao chép bất đồng bộ) | Failover: **đo được 4 đến 5 s** với `down-after` 3 s (manifest dùng 5 s, nên kỳ vọng ~6 đến 8 s); chủ động (`preStop`): ~12 s | Đã đo trên Redis/Sentinel thật ở máy dev, **chưa trên cluster thật** |
| Postgres, chỉ dùng CronJob `pg_dump` (repo này) | **Tối đa 6 giờ** + thời gian chạy job (mỗi bản là ảnh chụp tại một thời điểm, không có PITR) | Chưa biết cho dữ liệu thật. Diễn tập cục bộ: 0 đến 1 s, nhưng DB chỉ ~8 KB mỗi service (xem bên dưới), nên **con số này không nói gì về production** | Đã chạy trọn vẹn trên Postgres thật ở máy dev; **chưa trên cluster thật, chưa lên S3 thật** |
| Postgres managed + PITR (khuyến nghị) | Vài giây (WAL) | Phụ thuộc nhà cung cấp và kích thước | Không có trong repo; cần cấu hình |
| Kafka sang vùng dự phòng | Vài chục giây (kỳ vọng; MirrorMaker 2 bất đồng bộ) | Vài chục phút, phần lớn thao tác tay ở mục 4 | Mục tiêu, chưa đo |

## 0. Redis: Sentinel (losing a node, not a region)

Redis không cần sao lưu (xem bảng trên) nhưng cần **sẵn sàng**: khi master chết, Matching và Driver Location Service dừng. Mọi service dùng
`pkg/redisconn`: `REDIS_SENTINEL_ADDRS` (ConfigMap `grab-mini-config`, danh sách Sentinel) và `REDIS_MASTER_NAME` làm client hỏi Sentinel
master hiện tại và **tự theo failover**, không phải khởi động lại service. Mật khẩu qua `REDIS_PASSWORD_FILE` và `REDIS_SENTINEL_PASSWORD_FILE`
(cùng một Secret `grab-mini-redis`, key `password`, do bạn tạo).

**Manifest triển khai Redis** (`deploy/k8s/base/redis/`, sinh bởi `generate.py`, script ở `deploy/redis/`): StatefulSet 3 pod, mỗi pod là `redis`
+ `sentinel` + `redis_exporter` (quorum 2/3); PVC 5Gi mỗi pod, AOF `everysec`, `noeviction`; anti-affinity bắt buộc khác node và topologySpread bắt buộc khác zone;
PDB cho phép mất 1 pod; probe, request/limit, image ghim phiên bản; mật khẩu từ Secret; PodMonitor và 2 cảnh báo (`RedisNotExactlyOneMaster`, `RedisMemoryNearLimit`).
Ai là master do Sentinel quyết định lúc chạy chứ không cố định theo số thứ tự: pod khởi động lại hỏi Sentinel rồi vào làm replica của master hiện tại.
Muốn dùng Redis managed thay thế: bỏ `redis/` khỏi kustomization, để trống `REDIS_SENTINEL_ADDRS`, đặt `REDIS_ADDR`.

Đã kiểm chứng bằng `scripts/test-redis-ha.sh` (Redis 7.4 và Sentinel thật trong Docker, mỗi "pod" là hai container dùng chung network namespace, đúng các script mà ConfigMap mang): khởi động
lần đầu (redis-0 là master, 2 replica, 3 Sentinel thấy nhau, `CKQUORUM` OK, Sentinel trả **hostname** chứ không phải IP); mật khẩu bị ép (NOAUTH); sao chép ghi; **kill cả pod 0**, Sentinel
bầu master mới sau 4 đến 5 s mà không mất key; pod 0 quay lại thì **làm replica** của master mới và bắt kịp; không để lại Sentinel "ma" (id cố định); `preStop` chuyển vai trò master đi; và
`TestTheClientFollowsAFailover` (client Go thật, `pkg/redisconn`) chạy trên bộ 3 Sentinel này và pass. **Chưa kiểm chứng** vì không có cluster: lập lịch/anti-affinity/zone thật, PVC, DNS của headless
Service, probe do kubelet chạy, NetworkPolicy, và hành vi khi mất cả một zone. Lưu ý: NetworkPolicy mặc định chặn ingress vào mọi pod `part-of=grab-mini`, kể cả Redis; cần quy tắc cho phép
(service tới 6379/26379, giữa các pod Redis, Prometheus tới 9121) trước khi bật.

Sentinel adds availability, not capacity: there is still a single master. Replicas replicate asynchronously, so a failover can lose the last few seconds of writes
(location heals itself; but a lease or a `match:<orderId>` written last may also be lost, and the consequences of that, for example an order being matched twice, are **untested**).

## 1. Postgres

Nên dùng dịch vụ quản lý (RDS/Cloud SQL/…) bật **sao lưu tự động + PITR + replica đọc ở vùng khác**, hoặc operator (CloudNativePG, Crunchy) với
`barman`/`pgBackRest` đẩy WAL lên S3: chỉ những thứ đó cho PITR (RPO vài giây). Repo này có **lớp bảo hiểm thứ hai**, không phải thay thế:

**Sao lưu logic** (`deploy/k8s/base/backup/`, `scripts/pg-backup.sh`): CronJob `backup-order-service`, `backup-trip-service`, `backup-rating-service`, mỗi 6 giờ.
Init container (`postgres:16-alpine`) chạy `pg_dump -Fc` vào emptyDir rồi kiểm tra archive đọc được (`pg_restore --list`); container chính (`amazon/aws-cli`, ghim phiên bản)
đẩy lên `BACKUP_TARGET/<service>/<service>-<UTC>.dump`. Tên có timestamp UTC nên "mới nhất" là sắp xếp theo tên. Cấu hình: `BACKUP_TARGET`, `BACKUP_S3_ENDPOINT` (S3 không phải AWS),
`AWS_DEFAULT_REGION` trong ConfigMap `grab-mini-data-ops`; credential trong Secret `grab-mini-backup` (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`), bạn tạo. Giá trị mặc định của `BACKUP_TARGET`
là `s3://CHANGE-ME-...` để job **lỗi rõ ràng** chứ không lặng lẽ ghi vào đâu đó. Job chỉ ghi, không xóa: xóa bản cũ là lifecycle rule của bucket. Giới hạn:
mỗi dump là một ảnh chụp (một transaction), nên RPO là tối đa 6 giờ cộng thời gian job; `pg_dump` đọc cả DB nên đặt trên replica đọc nếu DB lớn; nếu ba service dùng chung một database thì
cùng nội dung được dump ba lần; `pg_dump` phải cùng hoặc mới hơn major version của server (hiện `postgres:16`); không mã hóa phía client (dựa vào mã hóa phía bucket); chưa có ai kiểm tra
nội dung ngoài diễn tập.

**Diễn tập khôi phục** (`scripts/pg-restore-drill.sh`, quy trình ở [runbook](runbook.md#backup-and-restore)): lấy bản mới nhất, khôi phục vào DB scratch, kiểm tra
phiên bản schema (`schema_migrations_<service>` phải bằng migration mới nhất của code, không có migration nửa vời), các bảng tồn tại, bảng chính không rỗng; in số dòng, kích thước, thời gian; tự dọn.
**Bắt buộc mỗi quý.** Sao lưu chưa từng được khôi phục thử là chưa có sao lưu.

**Đã chạy thật** (`scripts/test-data-ops.sh`, Postgres 16 thật trong Docker, migration thật của ba service): tạo dump hai lần, drill chọn đúng bản **mới hơn**
(5 đơn chứ không phải 6), schema ở phiên bản 7/8/2 như code, số dòng khớp DB sống, DB scratch được xóa; drill **fail** đúng khi sai phiên bản schema, thiếu dòng, hoặc không có bản sao lưu
(và vẫn dọn); và đường S3 đúng như CronJob chạy (dump bằng image postgres, upload bằng image `amazon/aws-cli` lên một S3 server tương thích, drill tải về và khôi phục).
Thời gian khôi phục đo được là **0 đến 1 giây**, nhưng trên dữ liệu ~8 KB mỗi dump: nó chỉ chứng minh quy trình đúng, **không phải RTO của production**.
Để có RTO thật: chạy drill trên bản sao lưu cỡ thật, ghi lại thời gian (tải + `pg_restore`; thường bị giới hạn bởi băng thông và xây index) và cộng với thao tác tay (dừng ghi, đổi Secret, bật lại, đối chiếu).
**Chưa kiểm chứng:** CronJob chạy trên cluster thật (kubelet, Secret, NetworkPolicy egress tới Postgres và S3, quota emptyDir), S3 thật của AWS, và drill ở kích thước production.

**Dọn dữ liệu:**
- Dòng dedupe: hai CronJob cũ trong `deploy/k8s/base/retention/` xóa `processed_events` (trip) và `rating_events` (rating) cũ hơn 30 ngày theo lô 10.000. 30 ngày là giới hạn dưới:
  Kafka giữ 7 ngày và một DLQ có thể chờ người xử lý; dưới mức đó, một redrive muộn có thể bị xử lý hai lần.
- **Đơn và chuyến đi** (mới): `retention-order-orders` (bảng `orders` của order-service) và `retention-trip-trips` (`trips` và `cancelled_orders` của trip-service), hằng ngày 03:43,
  theo lô 5.000, `scripts/pg-retention.sh`. Xóa dòng **đã kết thúc** cũ hơn `RETENTION_DAYS` (mặc định **180**, script từ chối < 30): đơn `CANCELLED`/`NO_DRIVER`; chuyến `COMPLETED`/`CANCELLED`
  (tuổi tính từ `completed_at`/`cancelled_at`, rơi về `created_at`); marker `cancelled_orders`. Đơn `MATCHED` là trạng thái cuối mà order-service biết (nó không được báo chuyến đã xong), nên chỉ bị xóa khi cũ hơn
  `MATCHED_ORDER_DAYS` (mặc định **365**, không thể còn chuyến đang chạy). **Không bao giờ** đụng `PENDING`, chuyến `ASSIGNED`/`IN_PROGRESS`, `outbox` (tự xóa khi publish),
  `outbox_dead` (cho người xử lý), `driver_reviews` (lịch sử đánh giá, không bị prune). Không có khóa ngoại giữa các bảng này (service tham chiếu nhau bằng id TEXT, mỗi service một DB), nên thứ tự duy nhất cần giữ là thời gian: mọi
  ngưỡng ≥ 30 ngày. Hệ quả cần biết: chuyến cũ hơn 180 ngày không còn đánh giá được (điểm đã nằm trong rating-service); **xóa là mất dữ liệu ở DB sống** nên bản lưu trữ dài hạn là bản sao lưu (đặt vòng đời bucket
  cho phù hợp pháp lý) chứ không có kho archive riêng. Đã chạy trên Postgres thật với 12.009 đơn và 8 chuyến đủ trạng thái/độ tuổi (xem `scripts/test-data-ops.sh`): xóa đúng từng dòng (12.003 đơn qua 3 lô, 4 chuyến, 1 marker),
  mọi dòng không kết thúc còn nguyên, lần chạy thứ hai xóa 0, dry-run chỉ đếm, cài đặt không an toàn bị từ chối. Giới hạn: không có index trên `created_at`, nên mỗi lô quét bảng; đủ cho hằng ngày, và nên
  thêm index (migration mới) nếu bảng lên hàng chục triệu dòng.

## 2. Kafka to the standby region (`deploy/k8s/dr/mirrormaker2.yaml`)

Apply it in the cluster of the **standby region**. It replicates `order.*` and `trip.*` (not `driver.location.*`: telemetry), keeps topic names unchanged
(`IdentityReplicationPolicy`) so consumers do not have to change configuration, and syncs consumer group offsets (`sync.group.offsets`).

Limitations to be aware of:

- Asynchronous: if the primary region dies, events not yet replicated are lost (RPO > 0). Postgres is the source of truth to reconcile against.
- Offsets are translated **approximately**, so consumers may reprocess a few messages just before the cutover point. The consumers are already idempotent (eventId,
  `match:<orderId>`); this is exactly why they need to be idempotent.
- There is no measurement of replication lag yet, and the manifest has only had its schema validated; it has **not yet been run on a real cluster**.

## 3. Cold archive on S3 (`deploy/k8s/dr/s3-archive.yaml`)

The Kafka Connect S3 sink writes the business topics to S3 as Avro, partitioned by hour. It is independent of both Kafka clusters and is meant for auditing and rebuilding
read models. It has also not yet been run on a real cluster; the S3 sink plugin is under the Confluent Community License.

## 4. Failover procedure

Only when the primary region will not recover soon. Each step should be rehearsed beforehand:

1. **Decide and announce.** Failing over has a cost (some unreplicated events are lost, offsets are approximate): it is decided by someone with the authority to do so.
2. **Stop writes in the primary region if it is still reachable** (scale to 0), to avoid both regions accepting orders.
3. **Postgres**: promote the replica in the standby region (or restore from PITR to the most recent point). Record the timestamp.
4. **Kafka**: check how far MirrorMaker 2 has caught up (the connector's `kafka_consumer_lag`, or compare end offsets); stop MirrorMaker 2.
5. **Schema Registry**: make sure all schemas are present (the `_schemas` topic was replicated, or re-register them).
6. **Redis**: bring up a fresh, empty instance. Drivers will report their locations again on their own; for the first few seconds Matching may return `NO_DRIVER`.
7. **Start the services** in the standby region (`kubectl apply -k deploy/k8s/base`, replace `DATABASE_URL`, set the Kafka bootstrap through the ConfigMap). Run
   `migrate` first.
8. **Switch DNS/gateway** to the standby region.
9. **Once stable**: reconcile the orders in Postgres against the Kafka events; `PENDING` orders that are too old need handling by hand (cancel or re-match). Run `dlq-redrive`
   if any dead letters appeared.

Returning to the primary region (failback) is a failover in the opposite direction: do it calmly, follow the steps above in reverse, and make sure the data in the two
regions has not diverged.

## Not done yet

- Chưa có diễn tập **chuyển vùng** nào, và chưa có diễn tập khôi phục trên cluster thật hay với dữ liệu cỡ production. Số đo duy nhất: khôi phục Postgres và failover Redis ở máy dev (xem bảng ở đầu); mọi RPO/RTO còn lại là mục tiêu.
- Chưa có kiểm tra tự động hằng tháng việc khôi phục (drill chạy tay từ máy có quyền; một CronJob cần một Postgres scratch riêng).
- Sao lưu Postgres không mã hóa phía client, không chống xóa (cần bucket có object lock/versioning do bạn cấu hình), và không có PITR.
- Redis không có sao lưu (cố ý); Redis Cluster không dùng được (xem docs/performance.md).
- Active-active nhiều vùng: không làm. Hai vùng cùng ghi Postgres và cùng ghép tài xế cần thiết kế riêng về xung đột.
