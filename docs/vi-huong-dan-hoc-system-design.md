# Hướng dẫn học system design qua grab-mini (tiếng Việt)

File này giúp bạn hiểu **toàn bộ project** và thấy **mỗi khái niệm system design nằm ở dòng code nào, đánh đổi (trade-off) cụ thể ra sao**. Nó bổ sung cho [vi-tong-hop-tai-lieu.md](vi-tong-hop-tai-lieu.md) (bản đồ tài liệu); file này là bản đồ **code**.

> Số dòng ghi theo commit lúc viết (tháng 11/2026). Code đổi thì số dòng có thể lệch vài dòng: tìm theo tên hàm là chắc nhất.

---

## Phần 0. Lộ trình học đề xuất (khoảng 2 tuần, mỗi ngày 1–2 giờ)

| Bước | Làm gì | Mục tiêu |
|---|---|---|
| 1. Chạy được | `pnpm install && pnpm dev:all`, đặt một chuyến theo [guide.md](guide.md#take-a-ride), mở Kafka UI (`localhost:8080`) | Thấy event chạy thật, không chỉ đọc |
| 2. Lần theo một chuyến đi | Đọc **Phần 1** bên dưới, mở từng file theo đúng thứ tự | Có "bản đồ trong đầu" của cả hệ thống |
| 3. Đọc ADR 0001 → 0014 | [docs/adr/](adr/README.md) | Hiểu *vì sao*, không chỉ *cái gì* |
| 4. Học theo chủ đề | **Phần 2**: mỗi chủ đề gồm Vấn đề → Giải pháp → Code → Trade-off → Tự kiểm tra | Nắm từng pattern đủ sâu để giải thích khi phỏng vấn |
| 5. Phá thử | **Phần 4**: các thí nghiệm | Thấy hệ thống chịu lỗi thế nào |
| 6. Nghĩ ở quy mô lớn | [scaling.md](scaling.md), [performance.md](performance.md) | Biết cái gì vỡ trước khi tải tăng 10 lần |

Mẹo đọc code: mỗi service Go là một `package main` nhỏ. Bắt đầu từ hàm `main()` để thấy service **tiêu thụ topic nào, phát topic nào, nối với DB nào**, rồi mới đi vào handler. Thư viện dùng chung nằm ở `services/pkg/` và chứa phần lớn các pattern quan trọng.

---

## Phần 1. Lần theo một chuyến đi từ đầu đến cuối

Đây là cách nhanh nhất để hiểu project. Mở từng file theo thứ tự:

```
Rider App
  │ 1. GET /quotes                     → pricing-service (giá + surge, quote ký HMAC)
  │ 2. POST /orders (+ Idempotency-Key) → order-service
  │        INSERT orders + INSERT outbox  (cùng 1 transaction)
  │ 3. outbox relay ──► [order.created]
  │                         ├─► matching-service: GEOSEARCH + Lua claim tài xế ──► [order.matched]
  │                         ├─► pricing-service: đếm cầu cho surge
  │                         ├─► payment-service: lưu bản sao giá đã khóa
  │                         └─► promotion-service: ghi nhận mã đã dùng
  │                  [order.matched]
  │                         ├─► order-service: PENDING → MATCHED
  │                         ├─► trip-service: tạo trip (ASSIGNED)
  │                         └─► notification-service: đẩy WebSocket cho rider/driver
  │ 4. Driver: pickup / complete       → trip-service ──► [trip.status.changed]
  │                         ├─► payment-service: tính tiền, ghi sổ cái kép ──► [payment.recorded]
  │                         ├─► trip-service: trả tài xế về AVAILABLE
  │                         └─► notification-service
  │ 5. Rider đánh giá + tip             → [trip.rated] ──► rating-service, payment-service
Driver App ── vị trí ──► location-gateway ──► [driver.location.updated] ──► driver-location-service (Redis GEO)
```

| Bước | File:dòng | Đọc để thấy |
|---|---|---|
| Báo giá | [`pricing-service/quotes.go`](../services/pricing-service/quotes.go), [`pkg/surge/quote.go:100`](../services/pkg/surge/quote.go) `Issue` | Quote là chuỗi ký HMAC, **server không lưu gì** |
| Nhận đơn | [`order-service/main.go:95`](../services/order-service/main.go) `handleCreateOrder` | Kiểm tra input, quote, promo, idempotency key |
| Ghi đơn + event | [`order-service/main.go:315-334`](../services/order-service/main.go) `insertOrderTx` | `INSERT orders` và `outbox.Add` trong **cùng transaction** |
| Đẩy event lên Kafka | [`pkg/outbox/outbox.go:211`](../services/pkg/outbox/outbox.go) `Flush` | Advisory lock, đọc theo `ORDER BY id`, xóa sau khi gửi |
| Ghép tài xế | [`matching-service/main.go:58`](../services/matching-service/main.go) `matchOrder` → [`redis.go:23`](../services/matching-service/redis.go) → [`pkg/driverstatus/driverstatus.go:191`](../services/pkg/driverstatus/driverstatus.go) | Tìm theo vòng, claim nguyên tử bằng Lua |
| Cập nhật đơn | [`order-service/outcomes.go:79`](../services/order-service/outcomes.go) | `UPDATE ... WHERE status = 'PENDING'`: chuyển trạng thái có điều kiện |
| Tạo trip | [`trip-service/consumer.go:49`](../services/trip-service/consumer.go) `handleOrderMatched` | Dedupe `processed_events` + `ON CONFLICT (order_id) DO NOTHING` |
| Trạng thái trip | [`trip-service/db.go:168`](../services/trip-service/db.go) `transitionTrip` | State machine bằng `UPDATE ... WHERE status = ANY($3)` |
| Thanh toán | [`payment-service/internal/app/settle.go:155`](../services/payment-service/internal/app/settle.go) `settle` | Sổ cái kép, idempotency key cho provider |
| Đẩy realtime | [`notification-service/hub.go:255`](../services/notification-service/hub.go) `push` | Redis pub/sub để instance nào giữ socket cũng nhận được |
| Vị trí tài xế | [`location-gateway/tracker.go:46`](../services/location-gateway/tracker.go) `decide` → [`pkg/driverlocation/driverlocation.go:86`](../services/pkg/driverlocation/driverlocation.go) `Update` | Lọc báo cáo thừa, bỏ vị trí cũ đến muộn |

Sau bước này, bạn nên tự vẽ lại được sơ đồ trên mà không nhìn.

---

## Phần 2. Từng khái niệm system design: code ở đâu, đánh đổi gì

Mỗi mục theo khung: **Vấn đề → Giải pháp → Code → Trade-off → Tự kiểm tra**.

### 2.1. Kiến trúc hướng sự kiện (event-driven) qua Kafka

- **Vấn đề.** Nếu Order gọi HTTP lần lượt sang Matching, Pricing, Payment, Notification... thì một service chậm hoặc chết sẽ làm hỏng việc đặt xe.
- **Giải pháp.** Service chỉ phát event; ai quan tâm thì tự tiêu thụ. Order Service không biết ai đọc `order.created`.
- **Code.**
  - Bảng "service nào nhận/phát topic nào": [specs/ride-booking-mini/design.md §2–3](../specs/ride-booking-mini/design.md).
  - Hàm `main()` của mỗi service, ví dụ [`trip-service/consumer.go:22`](../services/trip-service/consumer.go) `runOrderMatchedConsumer`.
  - Ngoại lệ duy nhất là một lời gọi đồng bộ, và nó **fail-open**: [`pricing-service/routing.go:134`](../services/pricing-service/routing.go) `tripDistanceKm` (gọi Routing với timeout 300 ms; lỗi thì tính theo đường thẳng và tăng metric `pricing_routing_fail_open_total`).
- **Trade-off.**
  - Được: tách rời, scale độc lập từng consumer group, thêm consumer mới mà không sửa producer.
  - Mất: **nhất quán cuối cùng (eventual consistency)**. Rider có thể thấy đơn vẫn `PENDING` trong vài trăm ms sau khi đã match. Debug khó hơn, cần tracing xuyên Kafka ([`pkg/tracing`](../services/pkg/tracing)).
  - Mất: client không thể "đợi kết quả" trong cùng request, nên cần WebSocket và cơ chế hỏi lại bằng REST (xem 2.10).
- **Tự kiểm tra.** Vì sao Pricing được phép gọi HTTP sang Routing, còn Order thì không được gọi Promotion lúc đặt xe? (Gợi ý: ADR 0011, và nếu Routing chết thì hậu quả là gì?)

### 2.2. Partition key và thứ tự

- **Vấn đề.** Kafka chỉ giữ thứ tự **trong một partition**. Lệnh hủy không được vượt lên trước lệnh tạo của cùng một đơn.
- **Giải pháp.** Key theo thực thể cần thứ tự: `orderId`, `tripId`, `driverId`. Không cần thứ tự toàn cục.
- **Code.**
  - Producer đặt key: [`order-service/main.go:329`](../services/order-service/main.go) `outbox.Add(..., []byte(orderID), ...)`.
  - Số partition: `deploy/k8s/base/kafka/topics.yaml` (12 cho order/trip, 48 cho location).
  - Bảng key và lý do: [ADR 0002](adr/0002-partition-keys-and-ordering.md).
- **Trade-off.**
  - Số partition là **trần song song** của một consumer group. Tăng sau này sẽ re-map key, làm vỡ thứ tự trong lúc chuyển.
  - Hot key (một tài xế cực nhiều event) không chia được. Với key "theo thành phố" thì đây sẽ là vấn đề thật.
  - Retry topic (2.5) cố ý **phá** thứ tự cho message bị retry. Vì vậy mọi handler đều kiểm tra trạng thái hiện tại thay vì tin vào thứ tự.
- **Tự kiểm tra.** Vì sao `trip.rated` dùng key `driverId` chứ không phải `tripId`?

### 2.3. At-least-once và idempotency (chống xử lý trùng)

- **Vấn đề.** Consumer có thể crash sau khi xử lý nhưng trước khi commit offset, nên message sẽ được giao lại. Exactly-once của Kafka không bao phủ được việc ghi vào Postgres/Redis.
- **Giải pháp.** Chấp nhận giao **ít nhất một lần**, và làm mọi handler **idempotent**. Repo dùng **5 kỹ thuật khác nhau**, mỗi kỹ thuật hợp với một chỗ:

| Kỹ thuật | Ở đâu | Khi nào dùng |
|---|---|---|
| Bảng dedupe theo `eventId`, ghi cùng transaction với thay đổi | [`trip-service/db.go:67-68`](../services/trip-service/db.go) `markEventProcessed` (`INSERT ... ON CONFLICT DO NOTHING`); tương tự `rating_events`, và [`payment-service/internal/app/settle.go:268`](../services/payment-service/internal/app/settle.go) | Thay đổi không tự idempotent (cộng điểm rating, cộng tiền) |
| Ràng buộc unique tự nhiên | [`trip-service/consumer.go:103-107`](../services/trip-service/consumer.go) `ON CONFLICT (order_id) DO NOTHING` | Thực thể có khóa nghiệp vụ (một order chỉ có một trip) |
| Chuyển trạng thái có điều kiện (compare-and-set) | [`order-service/outcomes.go:79`](../services/order-service/outcomes.go), [`trip-service/db.go:168`](../services/trip-service/db.go) | State machine: lần chạy thứ hai không khớp `WHERE status`, nên không làm gì |
| Ghi nhớ kết quả trước | [`matching-service/main.go:62`](../services/matching-service/main.go) `MatchedDriver` | Kết quả không tất định (giao lại có thể chọn tài xế khác) nên phải trả **cùng tài xế** |
| Idempotency-Key ở HTTP | [`order-service/idempotency.go`](../services/order-service/idempotency.go), dùng ở [`main.go:137-150`](../services/order-service/main.go) | Client bấm "Đặt xe" hai lần hoặc mạng retry |

- **Trade-off.**
  - Bảng dedupe **phình mãi**: hiện chưa có job dọn ([scaling.md §8](scaling.md)).
  - Mỗi handler phải được thiết kế cẩn thận; quên một chỗ là có bug trùng lặp khó tái hiện.
  - Đổi lại, hệ thống đơn giản hơn nhiều so với exactly-once/transaction phân tán.
- **Tự kiểm tra.** Tại sao Matching không dùng bảng `processed_events` mà phải lưu `order:{id}:match` trong Redis?

### 2.4. Transactional outbox

- **Vấn đề.** "Ghi DB rồi publish Kafka" có khe hở: crash giữa hai bước làm **mất event** (đơn không bao giờ được match). "Publish rồi ghi DB" thì tạo **event ma** cho dữ liệu không tồn tại.
- **Giải pháp.** Ghi event vào bảng `outbox` **trong cùng transaction** với thay đổi trạng thái; một relay đọc bảng và đẩy lên Kafka.
- **Code** (đọc kỹ, đây là pattern quan trọng nhất của repo):
  - Ghi: [`pkg/outbox/outbox.go:105`](../services/pkg/outbox/outbox.go) `Add`, được gọi tại [`order-service/main.go:329`](../services/order-service/main.go) và [`order-service/cancel.go:154`](../services/order-service/cancel.go).
  - Relay: [`outbox.go:211`](../services/pkg/outbox/outbox.go) `Flush`.
    - Dòng 219: `pg_try_advisory_xact_lock`, nên mỗi source chỉ một relay publish tại một thời điểm, giữ thứ tự commit.
    - Dòng 227: `ORDER BY id LIMIT $2`.
    - Dòng 262: xóa các dòng đã gửi **trong cùng transaction**.
  - Cách ly event "độc": [`outbox.go:299`](../services/pkg/outbox/outbox.go) `quarantine`, chuyển sang `outbox_dead` để không chặn các event phía sau. Hàm `isPermanent` (dòng 318) cố ý bảo thủ.
  - Giám sát: [`outbox.go:341`](../services/pkg/outbox/outbox.go) `observeBacklog` (tuổi của event cũ nhất), kèm alert trong `deploy/monitoring/alerts.rules.yaml`.
- **Trade-off** ([ADR 0004](adr/0004-transactional-outbox.md)):
  - Thêm **tối đa ~1 giây độ trễ** (chu kỳ poll). Có `Wake()` (dòng 140) để giảm độ trễ khi vừa ghi.
  - Postgres phải ghi thêm và xóa outbox, sinh dead tuples cần autovacuum.
  - Một relay mỗi source là **trần throughput** (khoảng 100 dòng × số lần poll mỗi giây).
  - Vẫn chỉ là at-least-once (relay có thể crash sau khi gửi, trước khi xóa), nên consumer vẫn phải dedupe.
  - Phương án thay thế là CDC (Debezium): không cần poll, nhưng thêm một hệ thống phải vận hành.
- **Tự kiểm tra.** Nếu bỏ advisory lock và chạy 3 relay song song, chuyện gì xảy ra với thứ tự event của một đơn?

### 2.5. Xử lý lỗi: retry tier và dead-letter topic

- **Vấn đề.** Retry tại chỗ thì một message lỗi **chặn cả partition**. Message hỏng vĩnh viễn thì retry bao nhiêu cũng vô ích.
- **Giải pháp.** Phân loại lỗi:
  - Lỗi vĩnh viễn đi thẳng DLQ.
  - Lỗi tạm thời chuyển sang `<topic>.retry.N` với độ trễ 5 giây, 1 phút, 10 phút, rồi mới vào DLQ.
- **Code.**
  - [`pkg/kafkaconsumer/kafkaconsumer.go:65`](../services/pkg/kafkaconsumer/kafkaconsumer.go) `DefaultRetryTiers`.
  - Dòng 421 `fail`: quyết định DLQ hay tầng retry tiếp theo.
  - Dòng 288 `runRetryTier`: reader của từng tầng, chờ đến khi message tới hạn.
  - Cách một lỗi "tự khai" là vĩnh viễn: interface `Permanent()` (dòng 50). Ví dụ:
    - [`pkg/eventcodec/eventcodec.go:54-58`](../services/pkg/eventcodec/eventcodec.go) `DecodeError`
    - [`payment-service/internal/domain/ledger.go:72`](../services/payment-service/internal/domain/ledger.go) `UnbalancedError`
    - [`order-service/outcomes.go:22`](../services/order-service/outcomes.go) `badOutcome`
  - Công cụ đưa message về lại topic: [`services/dlq-redrive`](../services/dlq-redrive), chạy bằng `make dlq-dry-run TOPIC=...`.
- **Trade-off** ([ADR 0006](adr/0006-retry-tiers-and-dead-letter-topics.md)):
  - Message được retry sẽ chạy **sau** các message mới hơn, tức mất thứ tự.
  - Độ trễ được hiện thực bằng việc reader *ngồi chờ*, chiếm một worker.
  - Redrive cố ý làm thủ công: nếu nguyên nhân chưa sửa thì DLQ chỉ đầy lại.
- **Tự kiểm tra.** Vì sao lỗi kết nối Schema Registry **không** phải `DecodeError`? (Đọc chú thích ở `eventcodec.go:49-53`.)

### 2.6. Hợp đồng event có thể tiến hóa: Avro và Schema Registry

- **Vấn đề.** Producer và consumer được deploy vào các thời điểm khác nhau. Thêm field mà consumer cũ không đọc được thì hệ thống vỡ.
- **Giải pháp.** Avro với Schema Registry ở chế độ `BACKWARD`: field mới bắt buộc có giá trị mặc định.
- **Code.**
  - Schema: [`pkg/events/schemas/order.created.avsc`](../services/pkg/events/schemas/order.created.avsc), dòng 15–19 là 4 lần tiến hóa (`vehicleType`, `surgeMultiplier`, `price`, `promoCode`/`discount`), field nào cũng có `default`.
  - Wire format (magic byte + 4 byte schema id + payload): [`pkg/eventcodec/eventcodec.go:173`](../services/pkg/eventcodec/eventcodec.go) `Encode`, dòng 195 `Decode`.
  - Test chứng minh message cũ vẫn đọc được: `TestCurrentOrderCreatedReadsMessagesWrittenBeforeVehicleType` trong [`eventcodec_test.go`](../services/pkg/eventcodec/eventcodec_test.go).
- **Trade-off.**
  - Schema Registry trở thành phụ thuộc lúc khởi động của **mọi** service.
  - Mọi thay đổi phải nghĩ trước về giá trị mặc định, và không được đổi tên hay xóa field tùy ý.
  - Đổi lại, có thể nâng cấp từng service độc lập, payload nhỏ hơn JSON.

### 2.7. Tranh chấp tài nguyên: claim tài xế nguyên tử bằng Redis + lease

Đây là phần "khó" nhất về concurrency, nên đọc thật kỹ.

- **Vấn đề.**
  1. Hai đơn ở gần nhau cùng chọn một tài xế gần nhất ("đọc rồi ghi" để cả hai cùng thắng).
  2. Matcher chết sau khi claim nhưng trước khi publish, làm tài xế kẹt `ASSIGNED` mãi mãi.
- **Giải pháp.** Một script Lua chạy **nguyên tử** trong Redis: duyệt danh sách ứng viên, claim người đầu tiên hợp lệ. Claim là một **lease** có hạn.
- **Code.**
  - [`pkg/driverstatus/driverstatus.go:191-222`](../services/pkg/driverstatus/driverstatus.go) `claimFirstForOrderScript`:
    - Dòng 195: `redis.call("TIME")`, dùng **đồng hồ của Redis** để tránh lệch giờ giữa các matcher.
    - Dòng 203–205: tài xế `ASSIGNED` nhưng lease đã hết hạn thì lại claim được (tự chữa lành).
    - Dòng 210: từ chối tài xế bị `suspended`.
    - Dòng 211–212: set `ASSIGNED` kèm hạn lease.
  - Gọi từ [`matching-service/redis.go:61`](../services/matching-service/redis.go).
  - Lease kết thúc khi Trip Service chuyển tài xế sang `ON_TRIP`. Hàm `Transition` (dòng 306) là compare-and-set: chỉ đổi nếu trạng thái hiện tại đúng như mong đợi.
- **Trade-off** ([ADR 0005](adr/0005-redis-atomic-driver-claim.md)):

| Phương án | Vì sao không chọn |
|---|---|
| `SELECT ... FOR UPDATE SKIP LOCKED` trên Postgres | Đúng và bền, nhưng Postgres không hợp với tìm kiếm địa lý ở 25.000 cập nhật/giây, lại đè tải lên DB của đơn hàng |
| Check-and-set từng ứng viên | Tối đa 50 round-trip mỗi đơn (đã đo trước khi chuyển sang một script) |
| Một matcher duy nhất | Đúng hiển nhiên nhưng không scale ngang |

  Cái giá phải trả:
  - Redis **không phải nguồn sự thật bền vững**. Trạng thái được coi là tự chữa lành (tài xế báo lại vị trí, lease hết hạn).
  - Script tự dựng key bên trong Lua nên **không chạy được trên Redis Cluster** nếu chưa có hash tag.
- **Tự kiểm tra.** Nếu lease dùng `time.Now()` của từng matcher thay vì `TIME` của Redis, kịch bản lỗi cụ thể là gì?

### 2.8. Race condition giữa hủy đơn và ghép tài xế

- **Vấn đề.** Rider bấm hủy đúng lúc Matching vừa tìm được tài xế.
- **Giải pháp, ba lớp chặn:**
  1. **Order Service**: hủy bằng `UPDATE ... WHERE id = $1 AND status = 'PENDING'` ([`order-service/cancel.go:145-146`](../services/order-service/cancel.go)), còn match cũng chỉ áp dụng khi đang `PENDING` ([`outcomes.go:79`](../services/order-service/outcomes.go)). Postgres đảm bảo chỉ một bên thắng.
  2. **Matching**: kiểm tra đã hủy chưa **trước và sau** khi claim; nếu đã hủy thì trả tài xế lại ([`matching-service/main.go:78-93`](../services/matching-service/main.go)).
  3. **Trip Service**: cả hai consumer (`order.matched` và `order.cancelled`) lấy cùng một advisory lock theo đơn ([`trip-service/cancel.go:158`](../services/trip-service/cancel.go) `lockOrder`), và bảng `cancelled_orders` (dòng 112) giúp một `order.matched` đến muộn không tạo trip.
- **Trade-off.** Logic rải ở ba service, khó nhìn tổng thể; đổi lại không cần lock phân tán. Đọc code này để thấy rằng **eventual consistency đòi hỏi suy nghĩ về mọi thứ tự event có thể xảy ra**.

### 2.9. Tìm kiếm địa lý và luồng vị trí tần suất cao

- **Vấn đề.** 25.000 cập nhật vị trí mỗi giây, và cần tìm tài xế gần nhất thật nhanh.
- **Giải pháp và code:**
  - **Gateway chắn trước Kafka** ([ADR 0009](adr/0009-location-gateway-and-telemetry-stream.md)): [`location-gateway/tracker.go:46`](../services/location-gateway/tracker.go) `decide` giới hạn tốc độ theo tài xế và bỏ báo cáo khi tài xế "không di chuyển" (dòng 63). Đây là **load shedding** ngay ở cửa vào.
  - **Độ bền thấp hơn có chủ đích**: [`location-gateway/main.go:191`](../services/location-gateway/main.go) dùng `RequiredAcks: kafka.RequireOne` (acks=1), retention 1 ngày. Mất vài vị trí không sao vì vài giây sau tài xế lại báo.
  - **Bỏ vị trí cũ đến muộn**: [`pkg/driverlocation/driverlocation.go:86`](../services/pkg/driverlocation/driverlocation.go) `Update` so timestamp; log `discarded as out-of-order` ở [`driver-location-service/main.go:71`](../services/driver-location-service/main.go).
  - **Tìm theo vòng mở rộng**: [`matching-service/redis.go:29-46`](../services/matching-service/redis.go) `searchRings` (1 km, 3 km, rồi bán kính cấu hình), vì chi phí `GEOSEARCH` tỉ lệ với **số tài xế trong bán kính** chứ không với tổng số tài xế.
  - **Shard theo ô địa lý** (mặc định tắt): `GEO_SHARD_DEGREES` trong [`driverlocation.go:28-60`](../services/pkg/driverlocation/driverlocation.go).
- **Trade-off.**
  - Trạng thái của gateway nằm trong bộ nhớ, nên nếu request của một tài xế rơi vào nhiều instance thì giới hạn bị lỏng. Cách giải là route theo `driverId` ở load balancer ([scaling.md §6](scaling.md)).
  - Shard không làm **một** truy vấn rẻ hơn; nó chỉ chia tải sang nhiều core ([performance.md](performance.md#redis-geo-is-sharding-needed)).
- **Tự kiểm tra.** Vì sao event giao dịch dùng `acks=all` còn vị trí dùng `acks=1`? Đây là ví dụ kinh điển của việc **chọn độ bền theo giá trị dữ liệu**.

### 2.10. Đẩy realtime: WebSocket, fan-out và đường khôi phục

- **Vấn đề.** Có nhiều instance Notification; socket của rider nằm ở instance A, nhưng event có thể được xử lý ở instance B.
- **Giải pháp.** Instance xử lý event `PUBLISH` lên kênh Redis `notify:rider:{id}`; instance nào giữ socket đã `SUBSCRIBE` kênh đó thì đẩy xuống.
- **Code.**
  - [`notification-service/hub.go:109`](../services/notification-service/hub.go) `channelFor`
  - Dòng 216: subscribe khi có socket
  - Dòng 255–260: `push` bằng `Publish`
- **Đường khôi phục** ([design.md §4d](../specs/ride-booking-mini/design.md)): push là **best-effort**. Khi app reload, nó hỏi lại REST (`GET /riders/{id}/latest-order`, `GET /drivers/{id}/active-trip`).
- **Trade-off** ([ADR 0008](adr/0008-notification-fan-out-and-recovery.md)):
  - Pub/sub không lưu message, nên client mất kết nối là mất push. Bù lại bằng REST.
  - Mỗi cập nhật vị trí tốn thêm Redis lookup và publish dù không ai xem ([scaling.md §5](scaling.md)), và đang dùng chung Redis với Matching.
- **Nguyên tắc rút ra.** **Kênh realtime là gợi ý, REST là nguồn sự thật.** Chat cũng theo nguyên tắc này ([ADR 0012](adr/0012-chat-push-is-best-effort-and-private.md)).

### 2.11. Surge pricing: điều khiển phản hồi, leader election, token không trạng thái

- **Code.**
  - Công thức: [`pkg/surge/surge.go:73`](../services/pkg/surge/surge.go) `Multiplier`: `clamp(1 + k·(cầu/cung − 1), 1, max)`.
  - Làm mượt: dòng 85 `Smooth`, kết hợp EMA và giới hạn bước thay đổi mỗi lần (dòng 91) để giá không nhảy giật.
  - **Leader election rẻ**: [`pricing-service/sampler.go:108`](../services/pricing-service/sampler.go) `SetNX(lockKey, ..., lease)`, nên chỉ một instance chạy sampler mỗi 5 giây.
  - **Quote không trạng thái**: [`pkg/surge/quote.go:100`](../services/pkg/surge/quote.go) `Issue` / dòng 134 `Verify`: body + HMAC. Order Service tự kiểm tra, **không cần DB chung, không cần gọi Pricing**.
  - **Fail-open**: Redis lỗi hoặc thiếu key thì hệ số là 1.0, kèm metric và alert `SurgePricingFailOpen`.
- **Trade-off.**
  - Fail-open nghĩa là khi sự cố có thể **mất doanh thu surge**, nhưng không chặn việc đặt xe. Đây là một lựa chọn kinh doanh.
  - Chia sẻ secret HMAC giữa hai service thì phải xoay khóa cẩn thận.
  - Lock `SETNX` có lease đủ cho tác vụ "chạy trễ một nhịp cũng không sao"; không đủ cho tác vụ cần đúng tuyệt đối.

### 2.12. Mã khuyến mãi: ký token để bỏ lời gọi đồng bộ

- **Code.**
  - [`pkg/promo/promo.go:109`](../services/pkg/promo/promo.go) `Issue`, dòng 120 `Verify`: token gắn với rider, giá và loại xe.
  - Kiểm tra "dùng một lần": [`order-service/promo.go:72-95`](../services/order-service/promo.go). Dòng 81 claim key trong Redis; **dòng 82–85: Redis lỗi thì vẫn cho giảm giá** (fail-open, đếm vào metric `unchecked`).
- **Trade-off** ([ADR 0011](adr/0011-signed-promo-token-instead-of-a-booking-time-call.md)):
  - Đặt xe không phụ thuộc Promotion Service.
  - Đổi lại, giới hạn tổng lượt dùng chỉ là **"mềm"**: Promotion ghi nhận sau qua event `order.created` và có thể ra trạng thái `OVER_LIMIT`.
  - Đây là ví dụ rõ của **availability vượt consistency** cho một tính năng mà sai lệch nhỏ chấp nhận được.

### 2.13. Tiền: sổ cái kép, số nguyên, idempotency với bên thứ ba

- **Code.**
  - Luật nghiệp vụ thuần: [`payment-service/internal/domain/ledger.go:77-96`](../services/payment-service/internal/domain/ledger.go) `Validate` (tổng các bút toán phải bằng 0).
  - Tiền là `int64` (VND), không dùng float: [`domain/money.go`](../services/payment-service/internal/domain/money.go). `Split` (dòng 78) chia tiền tài xế/nền tảng mà không mất đồng nào.
  - **Phòng thủ 2 lớp**: DB cũng kiểm tra bằng **deferred constraint trigger**, xem [`migrations/00001_create_ledger_and_payments.sql:38-56`](../services/payment-service/migrations/00001_create_ledger_and_payments.sql). `DEFERRABLE INITIALLY DEFERRED` cho phép chèn từng bút toán rồi kiểm tra lúc commit.
  - **Append-only**: dòng 61–76, chặn `UPDATE`/`DELETE`; sửa sai bằng một giao dịch đảo ngược.
  - `idempotency_key TEXT NOT NULL UNIQUE` (dòng 18).
  - Gọi provider: [`internal/app/settle.go:146-190`](../services/payment-service/internal/app/settle.go). Đọc chú thích dòng 146–154:
    - Đã ghi rồi thì không gọi provider nữa.
    - Lời gọi provider mang idempotency key, nên crash giữa "provider đã trừ tiền" và "đã commit DB" thì lần retry **không trừ hai lần**.
    - Bút toán được dựng và kiểm tra **trước** khi gọi provider (dòng 166–169).
- **Trade-off** ([ADR 0010](adr/0010-payment-ledger-and-provider-idempotency.md)):
  - Lời gọi provider nằm **ngoài** transaction DB, vì không thể giữ transaction trong lúc chờ mạng. Vì thế cần idempotency key.
  - Provider thật có độ trễ và rate limit riêng; timeout sẽ đẩy event vào retry tier.
  - Chưa có hoàn tiền, đối soát (reconciliation) hay chi trả cho tài xế.

### 2.14. Thứ tự sự kiện khi không tin được thứ tự giao: duyệt tài xế

- **Code.**
  - [`matching-service/review.go:48`](../services/matching-service/review.go) `applyDriverReview`.
  - Key `driver:{id}:review_at` ([`driverstatus.go:40`](../services/pkg/driverstatus/driverstatus.go)) lưu `occurredAt` của kết quả đã áp dụng gần nhất; kết quả cũ hơn hoặc trùng thì trả về `ReviewStale` (dòng 317–319).
- **Ý nghĩa.** Đây là **last-writer-wins theo timestamp nghiệp vụ**. Nó cần thiết vì retry topic có thể giao event cũ sau event mới ([ADR 0013](adr/0013-driver-eligibility-through-an-event-and-redis.md)).
- **Trade-off.** Producer (Profile Service) phải đảm bảo `occurredAt` **tăng nghiêm ngặt** theo từng tài xế.

### 2.15. Kiến trúc bên trong service: Clean Architecture (có chọn lọc)

- **Code.**
  - [`services/payment-service/internal/`](../services/payment-service/internal): `domain/` (chỉ thư viện chuẩn), `app/` (use case, `ports.go`), `adapters/` (http, kafka, postgres, provider, metrics).
  - Luật phụ thuộc được **ép bằng test**: [`internal/arch_test.go`](../services/payment-service/internal/arch_test.go). Domain import `net/http` hay `pgx` là test fail.
- **Trade-off** ([ADR 0014](adr/0014-clean-architecture-for-go-services.md)): bảng khảo sát trong ADR là phần đáng học nhất. Nó **không** áp dụng cho mọi service:
  - Matching: luật nghiệp vụ *chính là* script Lua; bọc interface quanh nó là đưa luật ra khỏi chỗ nó đúng.
  - Notification: gần như toàn hạ tầng, không có luật nghiệp vụ để bảo vệ.
  - Service dưới ~500 dòng: interface chỉ có một nơi gọi và một hiện thực, tức là tốn mà không được gì.

  Quy tắc rút ra: chỉ chia lớp khi có (a) luật nghiệp vụ phát biểu được mà không cần I/O **và** (b) nhiều hơn một loại I/O bao quanh.

### 2.16. Phân tích dữ liệu và vận hành

- **ksqlDB thay vì tự viết Analytics Service**: [`ksql/statements.sql:69`](../ksql/statements.sql) là stream-stream join `WITHIN 1 MINUTE` để đo độ trễ match. Trạng thái được dựng lại từ Kafka, không cần DB.
- **Autoscaling theo consumer lag** (không theo CPU): [`deploy/k8s/base/scaling/keda.yaml`](../deploy/k8s/base/scaling/keda.yaml) dùng `lagThreshold: '100'`.
- **Alert có unit test**: [`deploy/monitoring/alerts.rules.yaml`](../deploy/monitoring/alerts.rules.yaml) và `alerts.test.yaml` (`make prom-check`); mỗi alert có mục trong [runbook.md](runbook.md).
- **Batch timeout của Kafka writer**: [`pkg/kafkaconn/kafkaconn.go:190`](../services/pkg/kafkaconn/kafkaconn.go) `DefaultBatchTimeout = 2ms`. Mặc định của thư viện làm publish đồng bộ mất ~1 giây ([performance.md](performance.md)). Đây là đánh đổi **độ trễ lấy throughput**.

---

## Phần 3. Bảng tổng hợp trade-off (dùng để ôn phỏng vấn)

| Quyết định | Được | Mất | Khi nào xem lại |
|---|---|---|---|
| Event-driven qua Kafka | Tách rời, chịu lỗi, scale độc lập | Eventual consistency, khó debug | Cần phản hồi đồng bộ trong một request |
| Key theo thực thể | Thứ tự theo thực thể + song song | Trần partition, hot key | Cần song song vượt số partition |
| At-least-once + idempotent | Đơn giản, đúng | Mỗi handler phải cẩn thận; bảng dedupe phình | Dedupe quá đắt |
| Outbox + relay poll | Không mất event, không event ma | +1 giây độ trễ, tải Postgres, trần một relay | Backlog tăng: chia relay theo hash hoặc dùng CDC |
| Retry tier + DLQ | Không chặn partition | Mất thứ tự cho message retry | Topic cần retry mà vẫn giữ thứ tự |
| Avro BACKWARD | Nâng cấp độc lập | Phụ thuộc Registry, phải có default | Cần xóa hoặc đổi tên field |
| Claim tài xế bằng Lua + lease | Không giao trùng, không kẹt tài xế, 1 round-trip | Redis không bền, chưa chạy được trên Cluster | Một Redis không chịu nổi tải search |
| Vị trí acks=1, lọc ở gateway | Rẻ, chịu tải cao | Có thể mất vài vị trí | Vị trí trở thành dữ liệu tính tiền |
| Push best-effort + REST | Đơn giản | Client phải tự khôi phục | Cần đảm bảo giao (dùng hàng đợi bền) |
| Fail-open (surge, routing, promo) | Đặt xe không bao giờ bị chặn | Mất doanh thu hoặc giảm giá lố khi sự cố | Sai lệch trở nên đắt |
| Token ký (quote, promo) | Không gọi đồng bộ, không lưu trạng thái | Giới hạn chỉ "mềm", phải quản lý secret | Cần giới hạn cứng |
| Sổ cái kép + trigger | Tiền luôn cân, kiểm tra 2 lớp | Ghi nhiều hơn | Chuyển sang hệ thống tài chính chuyên dụng |
| Clean Architecture chọn lọc | Test luật không cần DB | Thêm lớp và interface | Service nhỏ mà phình ra |

---

## Phần 4. Thí nghiệm "phá thử" để hiểu sâu

Chạy `pnpm dev:all` trước. Chi tiết lệnh có trong [guide.md › Things to try](guide.md#things-to-try).

1. **Scale consumer group.** Chạy thêm một matching-service (`INSTANCE_ID=matching-B`), xem Kafka UI chia lại partition. Đơn có bị match hai lần không? Vì sao không (2.3, 2.7)?
2. **Gửi message không phải Avro** vào `driver.location.updated`. Nó đi thẳng vào `.DLQ` mà không qua retry, vì `DecodeError.Permanent()` (2.5).
3. **Replay topic.** Reset offset của `rating-service-group` về đầu: điểm trung bình không đổi (2.3).
4. **Đăng ký schema không tương thích.** Bị từ chối HTTP 409 (2.6).
5. **Cho mọi tài xế bận**, rồi đặt xe để thấy `order.no_driver_found`.
6. **Tự nghĩ thêm:**
   - Dừng Redis giữa chừng: surge về 1.0, promo `unchecked`. Xem metric ở `/metrics`.
   - Dừng Kafka: đơn vẫn được tạo, outbox tồn đọng; khởi động lại thì event được gửi (`make kafka-lag`).
   - Kill matching-service ngay sau khi claim: sau thời gian lease, tài xế lại `AVAILABLE`.
7. **Đọc test như đọc đặc tả.** Các file `*_test.go` cạnh mỗi pattern (ví dụ `pkg/outbox/outbox_test.go`, `pkg/driverstatus/driverstatus_test.go`) mô tả chính xác hành vi khi có lỗi. Chạy `make test`, hoặc `make test-integration` (cần Docker).

---

## Phần 5. Câu hỏi tự kiểm tra cuối cùng

Trả lời được hết các câu sau là bạn đã nắm project:

1. Vẽ luồng từ lúc rider bấm "Đặt xe" đến khi tài xế nhận được thông báo, ghi tên topic và nơi lưu trữ ở mỗi bước.
2. Kể 3 chỗ hệ thống có thể xử lý trùng một event, và kỹ thuật chống trùng ở mỗi chỗ.
3. Outbox giải quyết lỗi gì? Nó vẫn còn để lại vấn đề gì?
4. Hai matcher cùng chọn một tài xế: dòng code nào ngăn việc giao trùng?
5. Rider hủy đúng lúc tài xế được ghép: kể ba lớp phòng thủ.
6. Vì sao vị trí tài xế dùng acks=1 còn đơn hàng thì không?
7. Ở mức tải 10 lần, cái gì vỡ đầu tiên, và bạn sẽ đo gì để xác nhận ([scaling.md](scaling.md))?
8. Liệt kê các chỗ "fail-open" và giải thích vì sao Payment **không** fail-open.
9. Vì sao Matching không được tách thành Clean Architecture?
10. Nếu phải thêm field `paymentMethod` vào `order.created`, bạn làm những bước nào để không làm vỡ consumer nào?
