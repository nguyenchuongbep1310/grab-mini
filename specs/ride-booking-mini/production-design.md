# Production design: Ride-booking Mini on Kafka

Tài liệu này mô tả cách đưa grab-mini từ bản học tập (1 broker, 3 partition, chạy local) lên một hệ thống có thể chạy production. Nó **không đổi code**: mọi điểm "hiện tại" bên dưới đã được đối chiếu với code trong `services/`. Các con số tải là **giả định** để tính toán, không phải số đo.

Tham chiếu: [design.md](./design.md) (thiết kế bản học tập), [requirements.md](./requirements.md).

## 1. Mục tiêu và giả định

| | Giả định |
|---|---|
| Tài xế online cùng lúc | 100.000 |
| Tần suất báo vị trí | 1 lần / 4 giây, tức khoảng 25.000 msg/s |
| Đơn đặt xe đỉnh | 500 đơn/s |
| Kích thước event | ~150 byte (Avro) cho vị trí, ~300 byte cho các event khác |
| Độ khả dụng | 99,9% cho đường đặt xe (đặt, match, trip) |
| Độ trễ match | p95 dưới 3 giây từ `order.created` đến `order.matched` |
| Mất dữ liệu | Không được mất đơn, trip, rating. Được phép mất một phần vị trí (chỉ cần bản mới nhất) |

Từ giả định: luồng vị trí là ~2,5 MB/s, nhỏ so với năng lực Kafka. Vấn đề chính của production **không phải throughput** mà là **độ đúng đắn khi có lỗi** (mục 3) và **vận hành** (mục 6 đến 8).

## 2. Hai loại luồng, hai chính sách khác nhau

Gộp mọi topic vào một cấu hình là sai lầm phổ biến. Tách hai nhóm:

| | Luồng giao dịch (business events) | Luồng telemetry (vị trí) |
|---|---|---|
| Topic | `order.*`, `trip.*` | `driver.location.updated` |
| Mất message | Không chấp nhận | Chấp nhận được |
| Producer `acks` | `all` | `1` |
| `min.insync.replicas` | 2 | 1 |
| Idempotent producer | Bật | Không cần |
| Retention | 7 ngày (đủ để replay khi sự cố cuối tuần) | 1 ngày hoặc ít hơn |
| Nén | `lz4` | `lz4` hoặc `zstd` |
| Compaction | Không | Có thể dùng `cleanup.policy=compact` (key = driverId) nếu cần "vị trí mới nhất" khi consumer khởi động lại |

Hiện tại `kafka.Writer` trong mọi service không đặt `RequiredAcks`, nên dùng mặc định của kafka-go (`RequireNone`). Trên production phải đặt tường minh: `RequireAll` cho luồng giao dịch.

## 3. Độ đúng đắn: các lỗ hổng trong bản hiện tại và cách vá

Đây là phần quan trọng nhất. Mỗi mục có: hiện tượng, vị trí trong code, kịch bản hỏng, hướng xử lý.

### 3.1 Ghi DB rồi publish không atomic (dual write)

**Hiện tại.** `order-service/main.go` `handleCreateOrder`: `INSERT` order, sau đó mới `WriteMessages`. Nếu publish lỗi hoặc process chết giữa hai bước, đơn nằm `PENDING` mãi mà không ai match.

Tệ hơn ở Trip Service: `handleTransition` và `handleRateTrip` chỉ `log.Printf` khi publish lỗi rồi vẫn trả thành công. Trip đã sang `COMPLETED` trong DB nhưng `trip.status.changed` không bao giờ được gửi, nên rider không nhận thông báo và ksqlDB đếm thiếu. Với `trip.rated`, rating đã lưu vào `trips` nhưng Rating Service không bao giờ nhận được, nên điểm trung bình sai vĩnh viễn.

**Giải pháp: Transactional Outbox.**

```
BEGIN
  INSERT INTO orders ...
  INSERT INTO outbox (id, topic, key, payload, created_at) ...
COMMIT
        │
        ▼ (relay đọc outbox, publish, đánh dấu đã gửi)
     Kafka
```

- Bảng `outbox` nằm cùng database và cùng transaction với dữ liệu nghiệp vụ, nên hoặc có cả hai hoặc không có gì.
- Relay có hai lựa chọn: (a) goroutine poll bảng outbox theo lô (`SELECT ... FOR UPDATE SKIP LOCKED`), đơn giản; (b) CDC bằng Debezium đọc WAL của Postgres, độ trễ thấp và không tải DB, nhưng thêm một thành phần vận hành. Khuyến nghị bắt đầu bằng (a).
- Relay publish **at-least-once**, nên consumer phải idempotent. Bản hiện tại đã có `eventId` và dedupe, nên điều kiện này đã thỏa.
- Payload Avro đã được encode trước khi lưu, giữ được nguyên tắc "encode trước, ghi sau" hiện có.

### 3.2 Matching: claim tài xế rồi mới publish, và không idempotent khi giao lại

**Hiện tại.** `matching-service/main.go`: claim tài xế bằng Lua script rồi publish `order.matched`. Có hai lỗ hổng:

1. Nếu process chết **sau claim, trước publish** thì tài xế kẹt `ASSIGNED` mãi (code chỉ phục hồi khi publish trả về lỗi, không phục hồi khi process chết).
2. Nếu process chết **sau publish, trước commit offset** thì `order.created` được giao lại và matching chạy lần hai: claim thêm **một tài xế khác** cho cùng một đơn. Trip Service chặn được trip thứ hai nhờ `order_id UNIQUE`, nhưng tài xế thứ hai đã bị claim và không ai giải phóng.

**Giải pháp.**
- Lưu quyết định match theo `orderId` làm khóa idempotent: trước khi claim, `SET match:{orderId} <driverId> NX EX 3600`. Nếu khóa đã tồn tại, publish lại `order.matched` với tài xế đã lưu thay vì claim mới.
- Đặt TTL cho trạng thái `ASSIGNED` (ví dụ 60 giây). Hết hạn mà chưa có trip thì tài xế tự về `AVAILABLE`. Việc này cũng cứu trường hợp tài xế mất kết nối. Trip Service gia hạn hoặc đổi sang `ON_TRIP` khi tạo trip.
- Dùng outbox hoặc ghi `match:{orderId}` và event trong cùng một bước logic để khoảng hở nhỏ nhất.

### 3.3 Vòng retry chặn partition

**Hiện tại.** `pkg/kafkaconsumer`: retry bằng `time.Sleep` ngay trong vòng fetch (3 lần × 2 giây). Một message lỗi chặn mọi message sau nó trong cùng partition tới 6 giây. Với lỗi tạm thời kéo dài (DB sập), cả consumer đứng và lag tăng.

**Giải pháp: retry topic.**
- Lỗi tạm thời: publish sang `<topic>.retry.5s`, `<topic>.retry.1m`, `<topic>.retry.10m` rồi commit ngay, không chặn partition chính.
- Hết các bậc retry mới sang `<topic>.DLQ`.
- Lỗi vĩnh viễn (`Permanent()`) đi thẳng DLQ như hiện tại.
- DLQ có cảnh báo (mục 7) và công cụ **redrive** (đọc DLQ, publish lại về topic gốc sau khi sửa lỗi).

Lưu ý thứ tự: retry topic phá thứ tự trong partition. Với topic mà thứ tự quan trọng (`trip.status.changed`), consumer phải tự chống đảo thứ tự bằng cách so trạng thái (đã có sẵn trong Trip Service: chuyển trạng thái idempotent theo `fromStatuses`).

### 3.4 Consumer thoát cả process khi fetch lỗi

**Hiện tại.** `kafkaconsumer.Run` gọi `log.Fatalf` khi `FetchMessage` trả lỗi, kể cả khi `ctx` bị hủy (tắt máy bình thường). Điều này tạo exit code 1 khi shutdown và chưa có graceful shutdown.

**Giải pháp.** Khi `ctx.Err() != nil` thì thoát vòng lặp, commit offset đang xử lý, đóng reader và writer. Lỗi fetch khác thì retry với exponential backoff thay vì kết thúc process. `main` của mỗi service bắt `SIGTERM` (Kubernetes gửi khi rollout) để dùng `signal.NotifyContext`.

### 3.5 Notification Service không scale ngang được

**Hiện tại.** Mọi instance dùng chung `GroupID: "notification-service-group"`, nên Kafka **chia** partition giữa các instance. Rider kết nối WebSocket vào instance A nhưng event của họ có thể do instance B đọc, và B không có socket của rider đó, nên tin bị rơi. Ngoài ra bản đồ `orderRider` và `driverRider` nằm trong bộ nhớ: restart là mất, và `order.matched` đến mà chưa biết rider thì bị bỏ qua. Notification Service còn đang là điểm mất thông tin im lặng, vì `pushToRider` trả về khi không có socket mà không lưu lại.

**Giải pháp (chọn một).**

| Phương án | Cách làm | Đánh đổi |
|---|---|---|
| A. Fan-out qua pub/sub | Notification đọc Kafka trong group chung, tra `riderId` từ Redis, rồi `PUBLISH` lên Redis Pub/Sub kênh `rider:{id}`. Mọi gateway WebSocket subscribe kênh của các socket đang giữ. | Thêm Redis Pub/Sub, nhưng tách rõ "xử lý event" khỏi "giữ socket". **Khuyến nghị.** |
| B. Mỗi instance một group riêng | `GroupID` kèm instance id, mọi instance đọc mọi event, tự lọc theo socket đang giữ. | Đơn giản nhưng mỗi instance đọc toàn bộ lưu lượng, tốn khi lớn. Group rác tích lũy. |
| C. Sticky theo partition | Định tuyến rider vào đúng instance giữ partition của họ. | Rebalance làm đứt kết nối, phức tạp. |

Trong mọi phương án, ánh xạ `orderId → riderId` và `driverId → riderId` chuyển từ bản đồ trong RAM sang Redis (TTL vài giờ), hoặc đưa `riderId` thẳng vào `order.matched` và `trip.status.changed` để consumer khỏi phải tra. Cách thứ hai tốt hơn: **event mang đủ dữ liệu cho người đọc** (xem 4.2).

Ghi nhớ: WebSocket chỉ là kênh best-effort. Client luôn có đường khôi phục bằng REST (design.md mục 4d), và đó vẫn là cơ chế bảo đảm cuối cùng.

### 3.6 Exactly-once: cần hay không?

Không dùng Kafka transactions (EOS) cho toàn hệ thống. Lý do: side effect của chúng ta nằm ngoài Kafka (Postgres, Redis, WebSocket), nên EOS của Kafka không bao trùm được. Hướng đi đúng và rẻ hơn là **at-least-once + consumer idempotent + outbox**, vốn đã là mô hình của dự án. Chỉ cân nhắc Kafka transactions nếu có đoạn read-process-write thuần Kafka (ví dụ ksqlDB hoặc Kafka Streams, vốn đã tự lo).

## 4. Thiết kế Kafka

### 4.1 Topic, partition, replication

| Topic | Key | Partition (prod) | RF | Ghi chú |
|---|---|---|---|---|
| `order.created` | `orderId` | 12 | 3 | Tải thấp, 12 để dư chỗ scale Matching |
| `order.matched` | `orderId` | 12 | 3 | |
| `order.no_driver_found` | `orderId` | 6 | 3 | |
| `trip.status.changed` | `tripId` | 12 | 3 | |
| `trip.rated` | `driverId` | 6 | 3 | Key theo driver để Rating gom được theo tài xế |
| `driver.location.updated` | `driverId` | 48 | 3 (hoặc 2) | Tải cao nhất, xem 4.3 |
| `*.retry.*`, `*.DLQ` | như topic gốc | 3 | 3 | |

Cách chọn số partition: lấy **số consumer tối đa muốn chạy song song** làm trần, nhân thêm một hệ số dư. Không thể giảm partition sau này, và tăng partition làm đổi ánh xạ key → partition (phá thứ tự tạm thời), nên chọn dư từ đầu.

Cluster tối thiểu 3 broker, rải ra 3 availability zone, `broker.rack` được đặt để Kafka rải replica qua zone. KRaft với 3 controller riêng (không gộp vai trò với broker ở production).

### 4.2 Hợp đồng sự kiện

- **Đặt tên theo quá khứ, nói về sự thật đã xảy ra** (`order.created`, `trip.status.changed`). Đã đúng trong dự án.
- **Event mang đủ dữ liệu** để consumer không phải gọi ngược lại service khác. Ví dụ hiện tại `order.matched` không có `riderId`, buộc Notification phải nhớ ánh xạ trong RAM. Thêm `riderId` (trường mới có default rỗng, tương thích BACKWARD) giải quyết tận gốc.
- **Schema Registry** giữ chế độ `BACKWARD_TRANSITIVE` (kiểm tra với mọi phiên bản cũ, không chỉ bản liền trước), vì consumer có thể replay dữ liệu rất cũ.
- Đăng ký schema trong **CI/CD** (kiểm tra tương thích trước khi merge) thay vì để service tự đăng ký lúc chạy. Production tắt `auto.register.schemas`.
- Thêm vào `Envelope` các trường `schemaVersion` hoặc `traceId` để truy vết xuyên service.

### 4.3 Luồng vị trí: nút cổ chai thật

Hiện tại `driver-app-sim` publish thẳng lên Kafka vì đang giả lập. Trong production, app tài xế **không bao giờ** nói chuyện trực tiếp với Kafka. Cần một lớp:

```
Driver App ──HTTPS/gRPC/WebSocket──> Location Gateway ──batch──> [driver.location.updated]
```

- Gateway xác thực, giới hạn tần suất theo tài xế, validate tọa độ, gắn `timestamp` phía server nếu thiết bị nói dối.
- Producer đặt `linger.ms=20-50` và `batch.size` lớn, bật nén, để gom nhiều bản ghi vào một request.
- Consumer Driver Location Service dùng **pipeline Redis** (hoặc `GEOADD` hàng loạt) thay vì gọi một lệnh cho mỗi message, vì Redis sẽ là điểm nóng trước khi Kafka là điểm nóng.
- Có thể **bỏ qua cập nhật** nếu tài xế gần như không di chuyển (dưới 5m), giảm tải đáng kể.
- Redis GEO nên **shard theo khu vực** (ví dụ theo geohash hoặc thành phố) thay vì một key `drivers:geo` duy nhất cho cả nước. Một sorted set quá lớn làm `GEOSEARCH` chậm dần.

## 5. Dữ liệu và trạng thái

| Dữ liệu | Hiện tại | Production |
|---|---|---|
| Orders, Trips, Ratings | Một Postgres chung (cùng database `grabmini`) | **Mỗi service một database** (hoặc ít nhất một schema/user riêng) để thật sự độc lập. Primary + replica, failover tự động, PITR backup. |
| Trạng thái và vị trí tài xế | Redis đơn | Redis Cluster hoặc Sentinel. Chấp nhận mất vị trí khi failover vì sẽ tự phục hồi sau vài giây. **Trạng thái ASSIGNED/ON_TRIP** thì không thể coi là phù du, vì thế cần nguồn sự thật bền (Postgres) và Redis chỉ là cache có thể dựng lại từ `trip.status.changed`. |
| Bảng `processed_events` | Tăng vô hạn | Dọn theo lịch (xóa bản ghi cũ hơn thời gian retention của topic cộng biên an toàn). Phân vùng theo ngày để xóa nhanh. |
| Tạo bảng khi khởi động | `CREATE TABLE IF NOT EXISTS` trong `connectDB` | Dùng công cụ migration (golang-migrate hoặc goose) chạy trong pipeline deploy, không chạy DDL lúc service khởi động (tránh race khi nhiều instance cùng lên và tránh quyền DDL cho service). |

Vì nguồn sự thật của trạng thái tài xế nên là bền, cân nhắc đặt "tài xế đang bận" vào Postgres của Trip Service (cột `driver_id` + chỉ mục unique một phần `WHERE status IN ('ASSIGNED','IN_PROGRESS')`) để DB tự đảm bảo "một tài xế một trip đang chạy". Khi đó Redis chỉ còn dùng cho tìm kiếm không gian.

## 6. Triển khai và vận hành

### 6.1 Chạy trên Kubernetes

- Mỗi service một `Deployment`, 2 replica tối thiểu, `PodDisruptionBudget`, trải qua nhiều zone (`topologySpreadConstraints`).
- **Autoscaling theo consumer lag** (KEDA với scaler Kafka) thay vì theo CPU, vì consumer chờ I/O. Số replica bị chặn bởi số partition.
- Probe: `/health` hiện chỉ trả `ok`. Tách **liveness** (process còn sống) khỏi **readiness** (kết nối được Kafka, DB, Redis). Một service chưa kết nối được dependency không nên nhận traffic.
- Rolling update với `maxUnavailable: 0`. Nhờ schema tương thích và consumer idempotent, phiên bản cũ và mới chạy song song an toàn.
- Rebalance: dùng **cooperative sticky assignor** để scale/rollout không dừng toàn bộ group. Đặt `group.instance.id` (static membership) để restart nhanh không gây rebalance.

### 6.2 Bảo mật

- Kafka: TLS giữa client và broker, xác thực **SASL/SCRAM hoặc mTLS**, **ACL theo service** theo nguyên tắc ít quyền nhất (Order Service chỉ `WRITE order.created` và `READ` hai topic kết quả). Hiện tại mọi thứ là `PLAINTEXT` không xác thực.
- Schema Registry, ksqlDB, Kafka UI: nằm sau xác thực, **không mở ra internet**. Kafka UI và ksqlDB hiện mở cổng cho mọi người trong dev.
- API công khai: có xác thực thật (JWT). Hiện `riderId` lấy từ body request (`handleRateTrip` ghi chú rõ là không có auth). Phải lấy danh tính từ token và kiểm tra rider đó đúng là chủ của trip.
- WebSocket: xác thực lúc nâng cấp kết nối và kiểm tra `Origin` (hiện `CheckOrigin` luôn trả `true`).
- Dữ liệu cá nhân (vị trí, tên): xem xét mã hóa trường, và đặt retention ngắn cho topic vị trí vì đây là dữ liệu nhạy cảm.
- Secrets (mật khẩu DB, credential Kafka) đi qua secret manager, không để trong env mặc định như `grabmini:grabmini` hiện tại.

### 6.3 Sao lưu và khôi phục thảm họa

- Kafka không phải là backup. Retention hữu hạn. Với dữ liệu cần giữ lâu, đổ sang object storage bằng Kafka Connect S3 sink.
- MirrorMaker 2 sao chép sang cluster thứ hai (region khác) nếu cần DR đa vùng. Lưu ý offset consumer không tự trùng khớp giữa hai cluster.
- Postgres: backup liên tục (WAL archiving) và thử khôi phục định kỳ.
- Replay có chủ đích: reset offset của một consumer group (như bài thử rating trong README) là công cụ phục hồi hợp lệ **vì** consumer idempotent. Viết thành runbook.

## 7. Quan sát (observability)

**Metric cần có** (Prometheus, qua kafka exporter và metric của từng service):

| Nhóm | Metric | Cảnh báo khi |
|---|---|---|
| Consumer | `consumer lag` theo group và partition | lag tăng liên tục quá 2 phút |
| Consumer | tỷ lệ lỗi handler, số retry | tỷ lệ lỗi vượt ngưỡng |
| DLQ | số message đến `*.DLQ` | **bất kỳ** message nào trong luồng giao dịch |
| Broker | under-replicated partitions, ISR shrink | khác 0 |
| Producer | độ trễ publish, tỷ lệ lỗi | p99 tăng bất thường |
| Outbox | tuổi của bản ghi chưa gửi cũ nhất | quá 30 giây |
| Nghiệp vụ | thời gian từ `order.created` đến `order.matched`, tỷ lệ `no_driver_found` | p95 trên 3 giây, hoặc tỷ lệ tăng đột biến |

**Truy vết phân tán** (OpenTelemetry): truyền `traceId` qua header Kafka (`kafka.Message.Headers`), để theo một đơn từ HTTP đến WebSocket qua mọi service. **Log có cấu trúc** (JSON) với `orderId`, `tripId`, `eventId`, `topic`, `partition`, `offset` làm trường chuẩn, thay vì `log.Printf` văn bản tự do như hiện tại.

SLO gợi ý: 99,9% đơn tạo thành công, 95% đơn được match trong 3 giây (khi có tài xế), 99% thông báo trạng thái trip đến client trong 2 giây.

## 8. Kiểm thử trước khi lên production

- **Contract test cho schema**: kiểm tra mỗi phiên bản `.avsc` mới tương thích với bản cũ trong CI. Đã có mẫu trong `eventcodec_test.go`.
- **Integration test với Kafka thật** (Testcontainers): hiện test chỉ dùng fake Schema Registry, chưa có test cho đường publish và consume thật.
- **Kiểm thử hỗn loạn**: giết broker, giết consumer giữa lúc xử lý, làm chậm Redis, và kiểm tra các bất biến: không mất đơn, không hai trip cho một đơn, không tài xế kẹt, rating không đếm trùng.
- **Kiểm thử tải**: mô phỏng 25.000 msg/s vị trí và 500 đơn/s, đo lag và độ trễ match.

## 9. Lộ trình đề xuất

Thứ tự theo mức độ rủi ro, không theo độ hấp dẫn kỹ thuật:

| Giai đoạn | Việc | Vì sao trước |
|---|---|---|
| 1. Đúng đắn | Outbox cho Order/Trip (3.1); khóa idempotent và TTL cho matching (3.2); graceful shutdown (3.4); `acks=all` | Đang có thể mất event và kẹt tài xế một cách im lặng |
| 2. Chịu lỗi | Retry topic (3.3); notification scale được (3.5); migration DB; dependency health check | Tránh sự cố dây chuyền khi một thành phần chậm |
| 3. An toàn | Xác thực người dùng, TLS/ACL Kafka, secret manager, Location Gateway | Điều kiện để mở ra internet |
| 4. Vận hành | Cluster 3 broker nhiều zone, K8s, KEDA, metric/trace/alert, runbook | Cần có trước khi có lưu lượng thật |
| 5. Quy mô | Shard Redis GEO, sao lưu S3, DR đa vùng, kiểm thử tải | Làm khi số liệu thật cho thấy cần |

## 10. Những điều cố tình KHÔNG làm

- **Không** dùng Kafka làm cơ sở dữ liệu truy vấn cho API. Trạng thái hiện hành nằm trong Postgres, Kafka là đường truyền và nhật ký.
- **Không** chuyển sang microservice nhỏ hơn nữa. Sáu service hiện có đã đủ ranh giới; thêm nữa chỉ tăng chi phí vận hành.
- **Không** cho client kết nối Kafka trực tiếp (giữ nguyên nguyên tắc ở design.md mục 1).
- **Không** bật exactly-once toàn cục (xem 3.6).
- **Không** tăng partition "cho chắc" quá mức: mỗi partition tốn file handle, bộ nhớ broker và thời gian rebalance.

## 11. Trạng thái triển khai

### Giai đoạn 1 (đã làm)

| Mục | Kết quả | Nơi trong code |
|---|---|---|
| 3.1 Outbox | Order Service và Trip Service lưu event vào bảng `outbox` cùng transaction với thay đổi trạng thái (đơn, chuyển trạng thái trip, rating). Một relay publish sau khi commit, `Wake()` để gần như không thêm độ trễ. Mỗi service chỉ relay dòng của mình (`source`), một relay mỗi lần (advisory lock) nên thứ tự theo key được giữ. | `services/pkg/outbox`, `order-service/main.go` (`insertOrder`), `trip-service/main.go` (`transitionWithEvent`, `rateWithEvent`) |
| 3.2 Matching | `match:{orderId}` ghi quyết định match cùng lúc với claim (một Lua script). `order.created` bị giao lại thì dùng lại tài xế cũ. Trạng thái `ASSIGNED` là **lease** (mặc định 120 giây, `ASSIGN_LEASE_SECONDS`) chứ không phải TTL của key: hết hạn thì tài xế claim lại được. | `pkg/driverstatus` (`ClaimForOrder`), `matching-service/main.go` |
| 3.4 Shutdown | `kafkaconsumer.Run` thoát khi context bị hủy, commit message đã xử lý xong, không commit message đang dở, và không còn `log.Fatalf` khi fetch lỗi (backoff rồi thử lại). Mọi service bắt SIGTERM. | `pkg/kafkaconsumer`, `pkg/serve`, `main.go` của từng service |
| `acks=all` | Mọi writer của luồng giao dịch (outbox relay của Order/Trip, Matching, DLQ) dùng `RequireAll`. Vị trí tài xế dùng `RequireOne`. | |

Thay đổi hành vi cần biết:

- **`ON_TRIP` bắt đầu từ lúc có trip.** Trước đây tài xế ở `ASSIGNED` suốt chuyến đi. Vì `ASSIGNED` giờ là lease, Trip Service đặt `ON_TRIP` (và kết thúc lease) khi tạo trip từ `order.matched`.
- **Trip Service tạo trip và ghi `processed_events` trong một transaction.** Trước đây dedupe được ghi trước khi tạo trip, nên nếu việc tạo trip lỗi thì lần retry bị bỏ qua và trip không bao giờ được tạo.
- Một request tạo đơn trả `201` ngay khi transaction commit, kể cả khi Kafka đang lỗi. Event được gửi sau khi Kafka trở lại.

### Giải phóng tài xế khi xong chuyến (đã làm)

Trước đây việc đặt tài xế về `AVAILABLE` nằm trong request, ngoài transaction: process chết giữa lúc commit trạng thái trip và lệnh Redis thì tài xế kẹt `ON_TRIP`, và lease không cứu được. Giờ Trip Service còn consume `trip.status.changed` (group `trip-driver-release-group`) và giải phóng tài xế khi trip `COMPLETED` hoặc `CANCELLED`. Event được xếp hàng cùng transaction với trạng thái nên không thể mất. Đường trong request vẫn giữ làm đường nhanh.

Consumer kiểm tra hiện tại chứ không tin event, vì event có thể đến muộn hoặc bị phát lại:

- tài xế còn trip đang chạy trong DB thì giữ nguyên;
- việc giải phóng là compare-and-set `ON_TRIP` → `AVAILABLE` (`driverstatus.Transition`), nên nếu Matching vừa claim tài xế cho đơn mới (DB chưa có trip) thì claim đó không bị ghi đè.

Code: `trip-service/consumer.go` (`releaseDriverAfterTrip`), `pkg/driverstatus` (`Transition`).

### Giới hạn đã biết của giai đoạn 1

- **Relay là at-least-once**: publish xong mà chết trước khi xóa dòng thì event đi hai lần. Consumer đã dedupe theo `eventId` hoặc idempotent theo trạng thái.
- Bảng `outbox` dùng chung một Postgres cho cả hai service ở bản local; khi tách database theo service (mục 5) mỗi service có bảng riêng.
- Matching vẫn publish trực tiếp (không có DB để dùng outbox). Khoảng hở giữa claim và publish được đóng bằng bản ghi match và lease, không phải bằng outbox.
- Phần Kafka (relay, consumer) đã được kiểm thử trên broker thật ở giai đoạn 2; toàn bộ hệ thống chưa chạy end-to-end (xem phần kiểm thử bên dưới).

### Cách đã kiểm thử

Test tự động chạy trên Postgres 16, Redis 7 và **Kafka 3.8 thật** (KRaft, một node): relay (rollback, thứ tự, lỗi publish, nhiều relay, tách theo source, publish qua `kafka.Writer` thật), lease và claim theo đơn (kể cả 10 claim đồng thời), matching (giao lại, chưa announce, publish lỗi), Trip Service (tạo trip, replay không đảo trạng thái tài xế, transition và rating cùng event, giải phóng tài xế), và consumer (retry qua các tầng, DLQ, lỗi vĩnh viễn, không chặn partition, shutdown, message bị ngắt giữa chừng được giao lại). Các test này tự bỏ qua nếu không đặt `TEST_DATABASE_URL`, `TEST_REDIS_ADDR`, `TEST_KAFKA_BROKERS` (xem README).

**Chưa chạy toàn bộ hệ thống** (Schema Registry, ksqlDB, các app React) vì môi trường phát triển không có Docker daemon. Các service chưa được chạy cùng nhau end-to-end: cần `pnpm dev:all` và thử lại các bài trong README ("Things to try") trước khi merge.

## 12. Giai đoạn 2 (đã làm)

| Mục | Kết quả | Nơi trong code |
|---|---|---|
| 3.3 Retry topic | Với `RetryTiers`, lỗi tạm thời đưa message sang `<topic>.retry.1`, `.2`, `.3` (chờ 5 giây, 1 phút, 10 phút) và partition chạy tiếp. Mỗi tầng có reader riêng (group `<group>.retry.<n>`) chờ đến hạn rồi thử lại; hết tầng cuối thì sang DLQ. Lỗi vĩnh viễn vẫn đi thẳng DLQ. Handler luôn thấy `msg.Topic` gốc. Retry topic được tạo khi khởi động nếu chưa có. Order, Matching, Trip, Rating bật `DefaultRetryTiers`. | `pkg/kafkaconsumer` |
| 3.5 Notification scale ngang | Event mang `riderId` (`order.matched`, `order.no_driver_found`, `trip.status.changed`; trường mới có default `""` nên tương thích BACKWARD). Notification không ghi socket trực tiếp từ event Kafka: nó `PUBLISH` lên kênh Redis `notify:rider:<id>` / `notify:driver:<id>`, instance nào giữ socket (đã `SUBSCRIBE` lúc kết nối) thì ghi. Trạng thái định tuyến (order→rider cho event cũ, driver→rider cho vị trí) nằm trong Redis. | `notification-service/hub.go`, `consumer.go`; `matching-service`, `trip-service` (cột `trips.rider_id`) |
| Migration DB | Mỗi service có `migrations/*.sql` nhúng trong binary, chạy bằng goose, bảng version riêng (`schema_migrations_<service>`), khóa advisory để nhiều instance không chạy trùng. `<service> migrate` chạy rồi thoát; `RUN_MIGRATIONS=false` bỏ qua lúc khởi động. | `pkg/migrate`, `*/migrations/` |
| Health check | `/livez` (luôn 200) tách khỏi `/readyz` (kiểm tra Postgres/Redis/Kafka, mỗi check có timeout 2 giây, chạy song song, 503 kèm tên dependency lỗi). `/readyz` trả 503 ngay khi nhận SIGTERM để load balancer rút traffic trước. Matching và driver-location không có API HTTP nên chỉ mở probe khi đặt `HEALTH_PORT`. `/health` cũ giữ nguyên (liveness) cho `dev.sh`. | `pkg/health` |

Phát hiện khi kiểm thử trên Kafka thật:

- **`kafka.Reader` mặc định `MaxWait` 10 giây, và `Close()` chờ fetch đang treo.** Service mất tới 6 đến 10 giây để dừng, kể cả phần graceful shutdown của giai đoạn 1. Đặt `MaxWait` 500ms thì `Close()` mất vài mili giây.
- Một `Reader` có `GroupID` không tự tạo topic chưa tồn tại, nên `kafkaconsumer` tạo retry topic khi khởi động.

Thay đổi hành vi và đánh đổi cần biết:

- **Retry topic làm đảo thứ tự**: message được thử lại sau những message đứng sau nó. Các handler bật `RetryTiers` đã chịu được việc này (chuyển trạng thái được bảo vệ bằng trạng thái hiện tại, mọi event có `eventId`). Với `driver.location.updated` và Notification không bật retry tầng: thông báo gửi lại sau vài phút chỉ hiển thị trạng thái cũ.
- **Redis trở thành dependency của Notification Service** (trước đây chỉ Kafka). Tin nhắn đăng lên kênh Redis lúc không có ai subscribe sẽ bị bỏ, đúng ngữ nghĩa best-effort của WebSocket: app khôi phục qua REST sau khi kết nối lại (design.md mục 4d).
- Mỗi cập nhật vị trí tốn thêm một `GET` Redis (tra tài xế→rider) và tối đa hai `PUBLISH`. Ở mức 25.000 msg/s (mục 1) đây là chỗ cần đo; nếu thành vấn đề, thêm cache cục bộ cho ánh xạ tài xế→rider.
- Trip Service lưu `rider_id` trên trip; trip tạo trước thay đổi này có `rider_id` rỗng nên event của chúng dùng đường dự phòng (tra order→rider trong Redis).

## 13. Giai đoạn 3 (đã làm)

| Mục | Kết quả | Nơi trong code |
|---|---|---|
| Xác thực người dùng (6.2) | Mọi route của Order, Trip, Rating đòi JWT (EdDSA/Ed25519 với `kid`: service chỉ giữ khóa công khai lấy từ JWKS, ghim đúng một thuật toán, kiểm tra issuer, audience và hạn dùng; xem `docs/security.md`). Danh tính lấy từ token, không từ body: body ghi `riderId` khác với token thì 403. Kiểm tra quyền sở hữu: rider chỉ thấy đơn và trip của mình (người khác nhận 404 như thể không tồn tại); chỉ tài xế của trip được pickup và complete; chỉ rider của trip được chấm điểm; cả hai bên được hủy. | `pkg/auth`, `*/main.go` (`routes`) |
| Nhà phát hành token | `auth-service` (cổng 3005): chỉ là **giả lập identity provider cho dev**, cấp token cho bất kỳ ai xin. Từ chối khởi động khi `APP_ENV=production` trừ khi đặt `ALLOW_DEV_TOKENS=true`. Production phải thay bằng IdP thật. | `services/auth-service` |
| WebSocket (6.2) | Token đi qua subprotocol (`bearer, <token>`), không nằm trên query string nên không vào access log. Socket của rider X hay driver X chỉ mở được bằng token đúng vai trò và đúng id. Kiểm tra `Origin` theo `ALLOWED_ORIGINS` (bắt buộc khi production). | `notification-service/main.go` |
| Frontend | `@grab-mini/client` đăng nhập, gắn token vào mọi request (thử lại một lần khi 401) và vào WebSocket. | `packages/client/src/auth.ts` |
| Secret manager (6.2) | `NAME_FILE` hoặc `NAME`; mặc định cho dev chỉ dùng ngoài production. Áp dụng cho `DATABASE_URL`, `JWT_PRIVATE_KEY` (chỉ nhà phát hành), `KAFKA_PASSWORD`, `SCHEMA_REGISTRY_PASSWORD`. | `pkg/secret` |
| Location Gateway (4.3) | App tài xế không bao giờ nói chuyện với Kafka. Gateway (cổng 3006) xác thực tài xế, kiểm tra tọa độ, giới hạn 1 báo cáo mỗi giây mỗi tài xế (429 kèm `Retry-After`), bỏ qua báo cáo của tài xế đứng yên (dưới 5 m) trừ một nhịp mỗi 30 giây, bỏ qua timestamp thiết bị lệch hơn 30 giây so với đồng hồ server, publish theo lô 20ms với lz4 và `acks=1`. `driver-app-sim` giờ gọi gateway qua HTTP như một client thật. | `services/location-gateway` |
| TLS + SASL/SCRAM + ACL cho Kafka (6.2) | `pkg/kafkaconn` bật TLS và SASL qua biến môi trường (`KAFKA_SECURITY_PROTOCOL`, `KAFKA_USERNAME`, `KAFKA_PASSWORD[_FILE]`, `KAFKA_CA_FILE`); production từ chối kết nối plaintext trừ khi đặt `KAFKA_ALLOW_PLAINTEXT=true`. Mỗi service một principal; `scripts/kafka-acls.sh` cấp đúng quyền (ghi topic nó sản xuất, đọc topic nó tiêu thụ, và Read/Write/Describe/Create trên prefix `<topic>.` cho retry topic và DLQ của nó). `deploy/kafka-secure/start-local.sh` dựng một broker một node có cả ba lớp bảo mật để thử trên máy. | `pkg/kafkaconn`, `scripts/kafka-acls.sh`, `deploy/kafka-secure/` |

Đã kiểm chứng trên broker thật có TLS, SASL/SCRAM, ACL và `auto.create.topics.enable=false` (dựng bằng chính `start-local.sh`): consumer của matching-service chạy trọn đường lỗi (retry topic, DLQ) với đúng quyền của nó; order-service ghi vào `order.created` được nhưng bị từ chối khi ghi vào `order.matched`; rating-service không đọc được `order.created`; sai mật khẩu, user lạ và chứng chỉ broker không tin cậy đều bị từ chối; `trip-service` chạy với principal riêng và `/readyz` trả 200.

Phát hiện khi làm:

- `auto.create.topics.enable=false` là cấu hình đúng cho production, nhưng DLQ trước đây chỉ tồn tại nhờ auto-create của broker. `kafkaconsumer` giờ tự tạo cả DLQ (dùng quyền Create trên prefix `<topic>.`), như đã làm với retry topic.
- ACL prefix `order.created.` khớp `order.created.retry.1` và `order.created.DLQ` nhưng **không** khớp chính `order.created`, nên quyền trên topic gốc và quyền trên topic phụ tách bạch.

Giới hạn đã biết, chưa làm:

- **Token chỉ được kiểm tra lúc mở WebSocket.** Kết nối đang mở vẫn sống sau khi token hết hạn (1 giờ). Cần đóng socket khi token hết hạn, hoặc yêu cầu client gửi token mới.
- ~~HS256 dùng khóa chung~~ đã xử lý: chuyển sang EdDSA, service chỉ giữ khóa công khai (JWKS), HS256 bị loại bỏ.
- **Giới hạn tần suất của gateway nằm trong RAM mỗi instance.** Chạy nhiều instance mà không sticky routing thì một tài xế có thể báo nhanh hơn giới hạn (mỗi instance cho một lần). Cần Redis hoặc sticky theo tài xế khi scale.
- **Một tài xế đọc được mọi đơn nếu biết UUID của đơn** (cần để Driver App lấy điểm đón). UUID khó đoán, nhưng chặt hơn nếu Order Service lưu tài xế được ghép (đã nhận `order.matched`) và chỉ cho tài xế đó đọc; tôi chưa làm vì có race giữa việc ghi `driver_id` và việc Driver App lấy đơn ngay sau khi nhận tin.
- **Schema Registry, ksqlDB, Kafka UI vẫn chưa có xác thực** và vẫn mở cổng trong `docker-compose.yml` (mục 6.2 của giai đoạn này). Services hỗ trợ basic auth và https tới Schema Registry (`SCHEMA_REGISTRY_USER`/`PASSWORD`), nhưng registry trong `docker-compose.yml` vẫn mở. `docker-compose.yml` vẫn chạy Kafka PLAINTEXT cho dev.
- Redis và Postgres chưa bật TLS, và Redis chưa có mật khẩu (chỉ `DATABASE_URL` đi qua `pkg/secret`).
- Chưa có mTLS giữa các service, chưa mã hóa trường dữ liệu cá nhân, chưa đặt retention ngắn cho topic vị trí.
- Chưa chạy end-to-end cả hệ thống (Schema Registry, ksqlDB, các app) vì sandbox không có Docker. Gateway và các route xác thực được kiểm thử bằng test tự động và bằng binary thật với Postgres, Redis, Kafka; còn luồng trình duyệt (đăng nhập, WebSocket với subprotocol) mới được kiểm tra bằng typecheck, chưa chạy trong trình duyệt.

## 14. Giai đoạn 4 (đã làm)

| Mục | Kết quả | Nơi trong code |
|---|---|---|
| Log có cấu trúc (7) | `logging.Setup` đưa mọi dòng log, kể cả `log.Printf`, qua `slog`: JSON có `service` ở production. Dòng nằm trong một span có thêm `trace_id` và `span_id`. Các dòng chính của consumer có `topic`, `partition`, `offset`, `key` làm trường riêng. | `pkg/logging`, `pkg/kafkaconsumer` |
| Metric (7) | `/metrics` trên mọi service: HTTP theo route (không theo URL), publish theo topic, `kafka_consumer_messages_total{result=ok\|retried\|dead_lettered}`, `outbox_oldest_pending_age_seconds`, độ trễ ghép (SLO 3 giây), đơn tạo, trip chuyển trạng thái, vị trí theo kết quả, socket đang mở. | `pkg/metrics`, từng service |
| Truy vết (7) | W3C `traceparent` qua HTTP và header Kafka, kể cả qua outbox (lưu cạnh dòng, cột `headers`) và qua retry topic. Một request qua Postgres, Kafka thật và một retry topic là **một trace**, mọi consumer span là con của request span. Xuất OTLP khi đặt `OTEL_EXPORTER_OTLP_ENDPOINT`; lấy mẫu 100% (dev) hoặc 10% (production). | `pkg/tracing`, `pkg/outbox` |
| Cổng admin (6.1) | `HEALTH_PORT` chuyển `/livez`, `/readyz`, `/metrics` sang một cổng riêng và bỏ chúng khỏi mux công khai: ingress không thể lộ `/metrics`. | `pkg/health` (`Mount`) |
| Triển khai (6.1) | Dockerfile (distroless, non-root); Kustomize base sinh từ `generate.py`: Deployment (non-root, filesystem chỉ đọc, probe, `maxUnavailable: 0`, trải qua zone, PDB, `preStop`), Job migration, KEDA scale theo lag (chặn bởi số partition), NetworkPolicy, PodMonitor. | `deploy/` |
| Cluster Kafka (4.1) | Strimzi: 3 controller KRaft và 3 broker trải 3 zone, `replication.factor=3`, `min.insync.replicas=2`, không bầu leader không đồng bộ, không tự tạo topic. `KafkaUser` và ACL **đọc từ `scripts/kafka-acls.sh`**: một bảng quyền duy nhất. | `deploy/k8s/base/kafka/` |
| Cảnh báo (7) | 15 luật: dead letter (page với bất kỳ message nào), lag tăng, tỷ lệ lỗi, URP, thiếu broker, lỗi publish, outbox kẹt, dòng bị cách ly, SLO ghép 3 giây, tỷ lệ không có tài xế, burn-rate đa cửa sổ cho đặt xe. Mỗi luật có test ca "phải kêu" và ca "không kêu" chạy bằng `promtool test rules`. | `deploy/monitoring/` |
| Runbook | Một mục cho mỗi cảnh báo: kiểm tra gì, theo thứ tự nào, làm gì; cùng với deploy, replay, xoay vòng secret. | `docs/runbook.md` |
| Redrive DLQ (3.3) | `dlq-redrive` đẩy dead letter về topic gốc với đúng key và bytes; nhớ vị trí trong consumer group; `-dry-run`, `-error`, `-max`. | `services/dlq-redrive` |

Lỗi tìm thấy và sửa khi làm giai đoạn này:

- **`metrics.HTTP` làm hỏng WebSocket.** Response writer của nó che mất `http.Hijacker`, nên không socket nào mở được qua middleware.
  Tìm thấy khi chạy test của Notification Service qua đúng chuỗi middleware của production. (Một thay đổi giữa chừng tưởng
  vô hại sẽ làm Notification Service hỏng hoàn toàn.)
- **Outbox relay bị chặn bởi một event hỏng.** Một event Kafka không bao giờ nhận (quá lớn, ví dụ) làm mọi lần flush thất bại:
  các event sau bị kẹt và các event tốt trong cùng lô bị gửi lại mãi. Giờ lô lỗi được thử từng event; event bị từ chối vĩnh viễn
  chuyển sang `outbox_dead` (có metric và cảnh báo), lỗi tạm thời giữ lại và commit phần đã gửi.
- **Mất message khi retry topic và DLQ cùng từ chối ghi.** Consumer vẫn commit message đó. Giờ nó giữ partition lại và thử đến khi
  message nằm an toàn trên một trong hai (hoặc dừng khi tắt máy, để message được giao lại).
- DLQ và retry topic được tạo với `KAFKA_REPLICATION_FACTOR` (production: 3) thay vì luôn là 1.

Giới hạn đã biết của giai đoạn này:

- **Chưa chạy trên cluster thật.** Manifest đã qua kubeconform (strict, kèm schema CRD của Strimzi, KEDA, Prometheus Operator),
  kiểm tra chéo với code, và `promtool`. Chưa được áp dụng vào một cluster, nên cách Strimzi/KEDA thật sự hành xử, quyền
  RBAC, PVC theo zone, và Dockerfile (chưa build vì sandbox không có Docker daemon) đều chưa được kiểm chứng.
- Ngưỡng cảnh báo là ước lượng theo giả định ở mục 1, chưa được hiệu chỉnh theo lưu lượng thật.
- Không có dashboard Grafana, chưa có Postgres/Redis/Schema Registry/OpenTelemetry collector trong manifest (đặt qua `config.yaml`).
- Không có static membership (`group.instance.id`) cho consumer: `kafka-go` v0.4 không hỗ trợ, nên mỗi lần restart pod vẫn gây một rebalance.
- Mỗi Deployment dùng một ServiceAccount chung và không có egress NetworkPolicy.
- Matching và Location Gateway vẫn không có outbox (xem runbook).

## 15. Giai đoạn 5 (đã làm một phần)

Nguyên tắc của giai đoạn này là **đo trước, tối ưu sau**, nên phần lớn thành quả là công cụ đo và những gì nó tìm ra.

- **Kiểm thử tải** (`deploy/loadtest`, `services/loadtest`, `services/dev-registry`): chạy các service thật không cần Docker; sinh tải mở
  (đặt đơn + tài xế báo vị trí qua gateway), đọc quantile từ `/metrics`. Tìm ra và sửa: `BatchTimeout` 1 s mặc định của kafka-go (mọi publish
  đồng bộ ≥ 1 s), commit đồng bộ từng message (`CommitInterval`), consumer tuần tự (`Workers`, một worker mỗi partition vẫn giữ thứ tự),
  và tối đa 50 round trip Redis mỗi đơn khi tài xế gần đã bận (`ClaimFirstForOrder`, một script). Kết quả và diễn giải trung thực:
  [docs/performance.md](../../docs/performance.md): 250 đơn/s + 750 vị trí/s sạch trên một máy 4 core; 400 đơn/s thì máy bão hòa.
- **Redis GEO**: đã đo. Chi phí tìm kiếm tỉ lệ với số ứng viên trong bán kính, không phải tổng tài xế; đã làm tìm theo vòng mở rộng
  (1 → 3 → bán kính cấu hình). **Shard theo ô có sẵn nhưng tắt** (`GEO_SHARD_DEGREES`, mặc định 0): đã đo, nó không làm mỗi truy vấn
  rẻ hơn mà chỉ chia tải ra nhiều key, nên chỉ bật khi các key nằm trên nhiều node; chưa dùng được với Redis Cluster (xem docs/performance.md).
- **Dọn dữ liệu**: CronJob xóa `processed_events` và `rating_events` cũ hơn 30 ngày theo lô (đã thử trên Postgres thật); `outbox` đã tự
  xóa khi publish, `outbox_dead` để người xử lý.
- **Sao lưu và DR**: [docs/disaster-recovery.md](../../docs/disaster-recovery.md) (dữ liệu nào quan trọng đến đâu, quy trình chuyển vùng),
  `deploy/k8s/dr/mirrormaker2.yaml` và `s3-archive.yaml` (kiểm tra schema bằng `deploy/verify.sh`, **chưa chạy trên cluster thật**).

Giới hạn còn lại: chưa có số đo trên hạ tầng thật (mục tiêu 500 đơn/s và 25.000 vị trí/s chưa kiểm chứng); chưa diễn tập khôi phục/chuyển
vùng; Redis Cluster chưa hỗ trợ (script dựng key động); Sentinel được client hỗ trợ và đã thử failover thật, nhưng manifest không triển khai Redis; bảng `orders`/`trips` chưa có chính sách lưu trữ. Cùng với các giới hạn ở
mục 13 và 14 và việc nhỏ từ giai đoạn 2 (cache ánh xạ tài xế→rider nếu đo thấy cần).
