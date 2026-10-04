# Sao lưu và khôi phục thảm họa

Phạm vi: mất một vùng (region) hoặc mất dữ liệu. Mất một broker hoặc một zone đã được lo bởi cluster 3 broker 3 zone (xem runbook
"Kafka broker down"); tài liệu này không lặp lại.

## Dữ liệu nào quan trọng đến đâu

| Dữ liệu | Nằm ở | Mất thì sao | Bảo vệ |
|---|---|---|---|
| Đơn, chuyến đi, đánh giá, outbox | Postgres (order, trip, rating) | **Mất thật**: nguồn sự thật | Sao lưu Postgres + PITR (bên dưới) |
| Event nghiệp vụ (`order.*`, `trip.*`) | Kafka, giữ 7 ngày | Dựng lại được từ Postgres cho phần đã commit; consumer chậm hơn 7 ngày mất event | MirrorMaker 2 sang vùng dự phòng; lưu trữ S3 |
| Vị trí tài xế, trạng thái tài xế, lease | Redis | **Tự hồi phục**: tài xế báo lại mỗi vài giây; lease hết hạn thì tài xế nhận được đơn lại | Không cần sao lưu |
| Schema Avro | Schema Registry (topic `_schemas` của Kafka) | Producer/consumer mới không giải mã được | Nằm trong Kafka: được MirrorMaker 2 nếu thêm `_schemas` vào `topicsPattern`; hoặc đăng ký lại từ `services/pkg/events/schemas/` (các service tự đăng ký khi khởi động) |

Mục tiêu đề xuất (hãy chỉnh theo hợp đồng thật): **RPO** (mất dữ liệu tối đa) vài giây với Postgres có replica đồng bộ/PITR, vài chục giây với
Kafka sang vùng dự phòng (MirrorMaker 2 sao chép bất đồng bộ); **RTO** (thời gian khôi phục) vài chục phút, phần lớn là thao tác tay bên dưới.

## 1. Postgres

Không tự quản bằng CronJob `pg_dump`: dùng dịch vụ quản lý (RDS/Cloud SQL/…) bật **sao lưu tự động + PITR + replica đọc ở vùng khác**, hoặc
operator (CloudNativePG, Crunchy) với `barman`/`pgBackRest` đẩy WAL lên S3. `pg_dump` hằng ngày chỉ là lớp bảo hiểm thứ hai, không cho PITR.

Bắt buộc **diễn tập khôi phục** mỗi quý: dựng DB từ bản sao lưu, chạy `order-service migrate` để chắc schema khớp, kiểm tra số dòng. Sao lưu
chưa từng được khôi phục thử là chưa có sao lưu.

Dọn dữ liệu: `deploy/k8s/base/retention/` có hai CronJob xóa dòng dedupe cũ hơn 30 ngày (`processed_events` của trip, `rating_events` của
rating) theo lô 10.000 dòng. 30 ngày là giới hạn dưới: Kafka giữ 7 ngày và một DLQ có thể chờ người xử lý; dưới mức đó, một redrive muộn có thể
bị xử lý hai lần. Bảng `orders`, `trips` không tự xóa: lưu trữ chúng là quyết định về sản phẩm và pháp lý. `outbox` tự xóa khi publish xong;
`outbox_dead` giữ lại cho người xem (xem runbook), dọn tay sau khi xử lý.

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

- Chưa có diễn tập chuyển vùng hay khôi phục nào; mọi RPO/RTO ở trên là mục tiêu, không phải số đo.
- Redis không có sao lưu (cố ý) và cũng chưa có Sentinel/Cluster trong manifest.
- Active-active nhiều vùng: không làm. Hai vùng cùng ghi Postgres và cùng ghép tài xế cần thiết kế riêng về xung đột.
