# Sao lưu và khôi phục thảm họa

Phạm vi: mất một vùng (region) hoặc mất dữ liệu. Mất một broker hoặc một zone đã được lo bởi cluster 3 broker 3 zone (xem runbook
"Kafka broker down"); tài liệu này không lặp lại.

## Dữ liệu nào quan trọng đến đâu

| Dữ liệu | Nằm ở | Mất thì sao | Bảo vệ |
|---|---|---|---|
| Đơn, chuyến đi, đánh giá, outbox | Postgres (order, trip, rating) | **Mất thật**: nguồn sự thật | Sao lưu Postgres + PITR (bên dưới) |
| Event nghiệp vụ (`order.*`, `trip.*`) | Kafka, giữ 7 ngày | Dựng lại được từ Postgres cho phần đã commit; consumer chậm hơn 7 ngày mất event | MirrorMaker 2 sang vùng dự phòng; lưu trữ S3 |
| Vị trí tài xế, trạng thái tài xế, lease | Redis (một master + replica, Sentinel giám sát) | **Tự hồi phục**: tài xế báo lại mỗi vài giây; lease hết hạn thì tài xế nhận được đơn lại | Không cần sao lưu |
| Schema Avro | Schema Registry (topic `_schemas` của Kafka) | Producer/consumer mới không giải mã được | Nằm trong Kafka: được MirrorMaker 2 nếu thêm `_schemas` vào `topicsPattern`; hoặc đăng ký lại từ `services/pkg/events/schemas/` (các service tự đăng ký khi khởi động) |

RPO/RTO theo từng lớp, tách **đã đo** khỏi **mục tiêu** (chỉnh theo hợp đồng thật):

| Lớp | RPO (mất dữ liệu tối đa) | RTO (thời gian khôi phục) | Trạng thái |
|---|---|---|---|
| Redis mất 1 node | 0 đến ~1 giây ghi cuối (AOF `everysec`, sao chép bất đồng bộ) | Failover: **đo được 4 đến 5 s** với `down-after` 3 s (manifest dùng 5 s, nên kỳ vọng ~6 đến 8 s); chủ động (`preStop`): ~12 s | Đã đo trên Redis/Sentinel thật ở máy dev, **chưa trên cluster thật** |
| Postgres, chỉ dùng CronJob `pg_dump` (repo này) | **Tối đa 6 giờ** + thời gian chạy job (mỗi bản là ảnh chụp tại một thời điểm, không có PITR) | Chưa biết cho dữ liệu thật. Diễn tập cục bộ: 0 đến 1 s, nhưng DB chỉ ~8 KB mỗi service (xem bên dưới), nên **con số này không nói gì về production** | Đã chạy trọn vẹn trên Postgres thật ở máy dev; **chưa trên cluster thật, chưa lên S3 thật** |
| Postgres managed + PITR (khuyến nghị) | Vài giây (WAL) | Phụ thuộc nhà cung cấp và kích thước | Không có trong repo; cần cấu hình |
| Kafka sang vùng dự phòng | Vài chục giây (kỳ vọng; MirrorMaker 2 bất đồng bộ) | Vài chục phút, phần lớn thao tác tay ở mục 4 | Mục tiêu, chưa đo |

## 0. Redis: Sentinel (mất một node, không phải mất vùng)

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

Sentinel thêm tính sẵn sàng, không thêm dung lượng: vẫn một master. Replica sao chép bất đồng bộ nên lúc failover có thể mất vài giây ghi cuối
(vị trí tự bù; nhưng một lease hoặc `match:<orderId>` ghi cuối cũng có thể mất, và hệ quả của việc đó, ví dụ một đơn bị ghép hai lần, **chưa được kiểm thử**).

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

## 2. Kafka sang vùng dự phòng (`deploy/k8s/dr/mirrormaker2.yaml`)

Áp dụng trong cluster của **vùng dự phòng**. Sao chép `order.*` và `trip.*` (không sao `driver.location.*`: telemetry), giữ nguyên tên topic
(`IdentityReplicationPolicy`) để consumer không phải đổi cấu hình, và đồng bộ offset của consumer group (`sync.group.offsets`).

Giới hạn cần biết:

- Bất đồng bộ: vùng chính chết thì những event chưa kịp sao chép sẽ mất (RPO > 0). Postgres là nguồn sự thật để đối chiếu.
- Offset được dịch **xấp xỉ**, nên consumer có thể xử lý lại vài message ngay trước điểm chuyển. Các consumer đã idempotent (eventId,
  `match:<orderId>`), đây là lý do chúng cần idempotent.
- Chưa có số đo độ trễ sao chép, và manifest mới chỉ được kiểm tra schema, **chưa chạy thử trên cluster thật**.

## 3. Lưu trữ lạnh trên S3 (`deploy/k8s/dr/s3-archive.yaml`)

Kafka Connect S3 sink ghi các topic nghiệp vụ ra S3 dạng Avro, chia theo giờ. Độc lập với cả hai cluster Kafka, dùng cho kiểm toán và dựng
lại read model. Cũng chưa chạy thử trên cluster thật; plugin S3 sink theo giấy phép Confluent Community License.

## 4. Quy trình chuyển vùng (failover)

Chỉ khi vùng chính không phục hồi sớm. Mỗi bước nên được diễn tập trước:

1. **Quyết định và thông báo.** Chuyển vùng có chi phí (mất một phần event chưa sao chép, offset xấp xỉ): do người có thẩm quyền quyết định.
2. **Dừng ghi ở vùng chính nếu còn chạm được** (scale về 0), để tránh hai vùng cùng nhận đơn.
3. **Postgres**: promote replica ở vùng dự phòng (hoặc khôi phục từ PITR tới thời điểm gần nhất). Ghi lại thời điểm.
4. **Kafka**: kiểm tra MirrorMaker 2 đã bắt kịp tới đâu (`kafka_consumer_lag` của connector, hoặc so offset cuối); dừng MirrorMaker 2.
5. **Schema Registry**: bảo đảm có đủ schema (topic `_schemas` đã sao, hoặc đăng ký lại).
6. **Redis**: dựng mới, trống. Tài xế sẽ tự báo lại vị trí; trong vài giây đầu Matching có thể trả `NO_DRIVER`.
7. **Khởi động service** ở vùng dự phòng (`kubectl apply -k deploy/k8s/base`, thay `DATABASE_URL`, bootstrap Kafka qua ConfigMap). Chạy
   `migrate` trước.
8. **Đổi DNS/gateway** sang vùng dự phòng.
9. **Sau khi ổn**: đối chiếu đơn trong Postgres với event Kafka; đơn `PENDING` quá cũ cần xử lý tay (hủy hoặc ghép lại). `dlq-redrive`
   nếu có dead letter phát sinh.

Quay lại vùng chính (failback) là một lần chuyển vùng ngược, làm khi bình tĩnh, theo chiều ngược của các bước trên và cần đảm bảo dữ liệu hai
bên không rẽ nhánh.

## Chưa làm

- Chưa có diễn tập **chuyển vùng** nào, và chưa có diễn tập khôi phục trên cluster thật hay với dữ liệu cỡ production. Số đo duy nhất: khôi phục Postgres và failover Redis ở máy dev (xem bảng ở đầu); mọi RPO/RTO còn lại là mục tiêu.
- Chưa có kiểm tra tự động hằng tháng việc khôi phục (drill chạy tay từ máy có quyền; một CronJob cần một Postgres scratch riêng).
- Sao lưu Postgres không mã hóa phía client, không chống xóa (cần bucket có object lock/versioning do bạn cấu hình), và không có PITR.
- Redis không có sao lưu (cố ý); Redis Cluster không dùng được (xem docs/performance.md).
- Active-active nhiều vùng: không làm. Hai vùng cùng ghi Postgres và cùng ghép tài xế cần thiết kế riêng về xung đột.
