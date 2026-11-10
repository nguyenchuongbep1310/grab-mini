# Hướng dẫn cho nhà phát triển (tiếng Việt)

Bản dịch tiếng Việt của [guide.md](guide.md): cách chạy, khám phá và vận hành grab-mini. Muốn biết dự án là gì và vì sao nó được xây như vậy, hãy bắt đầu từ [README](../README.md) và các [architecture decision record](adr/README.md). Khi hai bản khác nhau, bản tiếng Anh [guide.md](guide.md) là bản đúng.

Một rider đặt xe, matching service tìm tài xế gần nhất, chuyến đi chạy qua vòng đời của nó, và cả hai app cập nhật theo thời gian thực. Mỗi bước giữa các service là một event Kafka, được serialize bằng Avro và kiểm tra theo schema trong Schema Registry. ksqlDB đọc cùng các topic đó để tính analytics trực tiếp.

```
Rider App ──POST──> Order Service ──> [order.created] ──> Matching Service ──> [order.matched] ──┬──> Trip Service ──> [trip.status.changed] ──┐
                                                                     └──────> [order.no_driver_found]  │                                    │
Driver App (simulated) ──> [driver.location.updated] ──> Driver Location Service ──> Redis GEO        └────────────────────────────────────┴──> Notification Service ──WebSocket──> Rider / Driver App
```

Yêu cầu, thiết kế và danh sách task nằm ở [specs/ride-booking-mini](../specs/ride-booking-mini).

## Bắt đầu nhanh

**Điều kiện cần:** Docker Desktop (đang chạy), Go 1.26+, Node 24 (22.22+ cũng được; Node 20 quá cũ so với bộ công cụ test), và pnpm 9 (`corepack enable`, hoặc `npm i -g pnpm`).

```bash
pnpm install
pnpm dev:all
```

`pnpm dev:all` (hoặc `make dev`, chạy [scripts/dev.sh](../scripts/dev.sh)) khởi động hạ tầng Docker (Kafka, Schema Registry, ksqlDB, Postgres, Redis, Kafka UI, Prometheus, Loki, Promtail, Grafana), chờ cho đến khi chúng khoẻ, seed năm tài xế, build và chạy mọi service Go cùng bộ giả lập tài xế, rồi khởi động các app. Chạy `make help` để xem tất cả target tự động hoá. Khi nó in `grab-mini is up`:

| Cái gì | URL |
|---|---|
| Rider App | http://localhost:5171 |
| Driver App | http://localhost:5172/?driver=driver-1 |
| Ops dashboard | http://localhost:5173 |
| Grafana (Metrics & Logs) | http://localhost:3000 |
| Prometheus | http://localhost:9090 |
| Loki | http://localhost:3100/ready |
| Kafka UI | http://localhost:8080 |
| Schema Registry | http://localhost:8081/subjects |
| ksqlDB | http://localhost:8088/info |

Ctrl-C dừng các service và app (hạ tầng Docker vẫn chạy). Log nằm ở `.dev/logs/<name>.log` và được Promtail đẩy sang Loki để bạn truy vấn trong Grafana (`{job="grab-mini"}`). Dùng `make dev-reset` (`scripts/dev.sh --reset`) để xoá sạch dữ liệu trước khi chạy, và `make infra-down` (`docker compose down`) để dừng hạ tầng.

### Đi thử một chuyến

1. Trong **Rider App**, chọn **Car** hoặc **Bike**, chạm lên bản đồ để thả ghim điểm đến, rồi **Book ride**. Cạnh giá, app hiện độ dài lộ trình và thời gian ước tính ("3.2 km · about 8 min"); sau khi được ghép, nó hiện còn bao lâu nữa tài xế đến, rồi bao lâu nữa bạn tới điểm đến (lấy từ Routing Service, một mô hình không có dữ liệu bản đồ thật; nếu service đó chết thì dòng này đơn giản là bị ẩn).
2. Rider được ghép với tài xế gần nhất cùng loại xe và có thể xem tài xế di chuyển trên bản đồ. Năm tài xế được seed quanh trung tâm TP. Hồ Chí Minh: `driver-1` đến `driver-3` lái ô tô, `driver-4` và `driver-5` lái xe máy. Họ cũng có hồ sơ đã được duyệt (`profile-service seed-dev`, chỉ dùng cho development; tên, biển số và xe đều là dữ liệu bịa, và `rider-1` đến `rider-3` cũng có hồ sơ), nên thẻ "đã ghép" trong Rider App hiện tên tài xế, xe (mẫu, màu, loại) và biển số thay vì id (hỏi một lần cho mỗi tài xế; nếu tra cứu thất bại hoặc tài xế chưa có hồ sơ được duyệt, thẻ hiện id như trước và chuyến đi không bị ảnh hưởng), và avatar trên header mở **Your profile**, nơi rider sửa tên và số điện thoại (số điện thoại được lưu đúng như gõ và không được xác minh; avatar hiện chữ cái đầu của tên); một tài xế chưa được duyệt sẽ không được đề nghị chuyến, nhưng một tài xế được đặt thẳng vào Redis, như các test tải và test end-to-end làm, vẫn được ghép như trước. Ba mã khuyến mãi demo cũng được seed (`promotion-service seed-dev`, chỉ dùng cho development): `WELCOME10` (giảm 10%, tối đa 20.000 VND, cước từ 40.000 trở lên, mỗi rider một lần), `FLAT15K` (giảm 15.000 VND cho cước từ 40.000 trở lên, 5 lần mỗi rider) và `BIKE5K` (giảm 5.000 VND cho chuyến BIKE). Để dùng một mã, chạm **Have a promo code?** dưới giá trong Rider App (nó xuất hiện khi giá đã hiện), gõ `WELCOME10` rồi **Apply**: app hiện số tiền được giảm và tổng mới, và mã được khoá vào đơn khi bạn đặt xe. Một mã không hợp với chuyến (ví dụ cước thấp hơn mức tối thiểu, hoặc `BIKE5K` cho xe hơi) sẽ bị từ chối kèm lý do, và nếu bạn đổi điểm đến hoặc loại xe, mã được kiểm tra lại với giá mới. Biên lai sau chuyến hiện cước, dòng khuyến mãi và số tiền đã tính. Bạn cũng có thể thử các mã bằng các lệnh API ở mục "Promotion Service" bên dưới.
3. Mở **Driver App với tư cách tài xế đó**, ví dụ `http://localhost:5172/?driver=driver-2`. **Confirm pickup**, rồi **Complete trip**, và timeline của rider chạy theo.
4. Quay lại **Rider App**, đánh giá tài xế (1 đến 5 sao) rồi gửi. Bạn cũng có thể thêm nhận xét và tiền tip (0, 10k, 20k, 50k hoặc số tuỳ ý, VND). Tip chỉ được ghi lại, không có hệ thống thanh toán: Rating Service giữ tổng theo từng tài xế và app của tài xế hiện nó trong toast. App của tài xế hiện một toast và điểm trung bình mới.
5. Khi chuyến đang diễn ra (đã gán tài xế, hoặc đang đi), cả hai app đều hiện nút **Chat**. Chạm nó trong Rider App và trong Driver App, gõ tin nhắn rồi nhấn Enter (hoặc **Send**): phía bên kia thấy một huy hiệu với số tin chưa đọc trên nút Chat của họ, và tin nhắn hiện ra khi họ mở khung chat. Tin nhắn dài tối đa 500 ký tự trên một dòng. Tin không gửi được hiện "Failed - retry" (thử lại không thể gửi hai lần), gửi quá nhanh sẽ tạm khoá nút vài giây, và khi server đóng chat (10 phút sau khi chuyến kết thúc), khung chat chuyển sang chỉ đọc với dòng "This chat has ended". Các app nói chuyện với Chat Service qua proxy `/chat-api` (cổng 3011) và nhận tin của người kia dưới dạng push `chat.message.sent` trên cùng WebSocket; nếu socket rớt, lịch sử được tải lại khi nó kết nối lại.
6. Nếu bạn tải lại Rider App (hoặc push "trip completed" bị lỡ), một chuyến còn đang diễn ra sẽ trở lại như trước, và một chuyến đã hoàn thành trong vòng 24 giờ qua (`completedAt` của Trip Service) mà bạn chưa đánh giá hay đóng cũng vậy: bạn thấy lại biên lai và lời nhắc đánh giá. Khi bạn đánh giá hoặc chạm **New ride**, Trip Service lưu trạng thái đã đóng đó, nên nó đúng trên mọi tab và thiết bị, và lần tải lại sau đó bắt đầu từ màn hình đặt xe (chuyến bị huỷ hoặc không có tài xế thì không bao giờ được đưa lại).

Cũng nên thử: đánh giá một chuyến hai lần (lần hai bị từ chối), đặt xe máy (chỉ `driver-4`/`driver-5` nhận được), **Cancel trip** từ cả hai phía, hoặc đặt xe khi mọi tài xế đều bận (bạn nhận "No drivers nearby" sau các lần thử lại của matching service). `?rider=rider-2` chọn một rider khác.

Nền bản đồ lấy từ tile OpenStreetMap, nên cần có internet; các marker thì chạy không cần mạng.

### Trở thành tài xế

Ai cũng có thể đăng ký; admin quyết định. Thử với một driver id chưa có hồ sơ:

1. Mở Driver App như một tài xế mới, `http://localhost:5172/?driver=driver-9`. Thay vì "You're online", nó hiện **Become a driver**: điền tên, số điện thoại (tuỳ chọn), loại xe, biển số (ví dụ `59-X1 123.45`), và tuỳ chọn mẫu xe và màu, rồi **Submit application**. Mỗi trường hiện lỗi riêng nếu bị từ chối.
2. App giờ hiện **Waiting for review** và kiểm tra lại mỗi 10 giây. Khi tài xế chưa được duyệt thì không hiện chuyến nào.
3. Mở [Ops dashboard](http://localhost:5173), cuộn xuống **Driver applications** (dưới Promotions): đơn đăng ký được liệt kê cùng tên, xe, biển số và thời điểm gửi. Bấm **Approve** rồi **Confirm approve driver-9**. (**Reject** hỏi một lý do, tài xế sẽ đọc được; Driver App khi đó hiện nó cùng một form **Fix and re-apply**.)
4. Trong vòng 10 giây, Driver App mở khoá và hiện "You're online". Một tài xế đã duyệt có thể sửa mẫu xe và màu từ nút avatar; đổi **biển số hoặc loại xe** sẽ đưa hồ sơ về lại trạng thái chờ duyệt (không có chuyến cho đến khi admin duyệt lại). **Suspend a driver** trong cùng bảng đưa một tài xế đã duyệt ra khỏi hoạt động kèm lý do, tài xế sẽ thấy; danh sách **Suspended** của bảng có **Reinstate**.

Driver app không báo "approved" nếu profile service không liên lạc được: một tài xế hiện có thấy app bình thường với một thông báo nhỏ, và một lần từ chối (403 hoặc 404) hiện "Can't verify your driver account". Cổng chặn thật sự là Matching Service, chỉ đề nghị chuyến cho tài xế đã duyệt ([production-design §20](../specs/ride-booking-mini/production-design.md)).

## Xem Kafka hoạt động

Mở [Kafka UI](http://localhost:8080), cluster **grab-mini**, trong lúc bạn đi một chuyến:

- **Topics**: `order.created`, `order.matched`, `order.no_driver_found`, `trip.status.changed` và `driver.location.updated`, mỗi topic có 3 partition. Mở tab **Messages** của một topic: Kafka UI giải mã giá trị Avro qua Schema Registry, nên bạn thấy JSON. `driver.location.updated` tăng lên mỗi vài giây.
- **Partition key**: các message của cùng một đơn (hoặc một tài xế) luôn rơi vào cùng một partition, nhờ vậy giữ được thứ tự. So sánh cột **Partition** giữa các message.
- **Schema Registry** (mục menu trong Kafka UI, hoặc `curl localhost:8081/subjects`): mỗi topic một subject (`order.created-value`, ...), mỗi subject giữ schema Avro mà producer đã đăng ký.
- **Consumers**: mỗi service một consumer group (`matching-service-group`, `trip-service-group`, `driver-location-service-group`, `notification-service-group`). Mỗi group đọc độc lập mọi event trên các topic của nó. Xem **lag** ở đây: nó nằm ở hoặc gần 0 (một hai message đang bay trên `driver.location.updated` khi bộ giả lập đang publish; `N/A` chỉ có nghĩa là một group chưa commit offset trên topic đó).

## Analytics với ksqlDB

[Ops dashboard](http://localhost:5173) hiện các số liệu trực tiếp (đơn mỗi phút theo loại xe, tỷ lệ ghép, thời gian ghép trung bình, số chuyến hoàn thành, điểm đánh giá tài xế, thông lượng cập nhật vị trí). Bên dưới là bảng **Promotions**, nơi admin quản lý mã khuyến mãi: danh sách hiện giảm giá của từng mã, áp dụng cho ai, thời hạn, mức dùng so với giới hạn và trạng thái; **New promotion** tạo mã mới, và **Disable** chặn các lần dùng mới. Bảng này nói chuyện với Promotion Service chứ không phải ksqlDB, và đăng nhập là admin qua bộ phát token development (`?admin=<id>` chọn id admin, mặc định `ops-1`). Bên dưới nữa, **Driver applications** là hàng đợi duyệt (Profile Service): các tài xế đang chờ duyệt cùng tên, xe, biển số, số điện thoại như đã gõ (không được xác minh) và thời điểm đăng ký, cũ nhất trước (**Load more** duyệt tiếp qua cursor), **Approve** và **Reject** (bắt buộc có lý do và tài xế sẽ thấy, nên đừng đưa dữ liệu cá nhân của người khác vào), một form để **Suspend** một tài xế đã duyệt theo id kèm lý do, và một chế độ xem **Suspended** với **Reinstate**. Mọi quyết định đều đòi click lần hai, danh sách được làm mới sau đó, và một quyết định không còn phù hợp (admin khác đã làm trước) sẽ nói tài xế hiện đang ở trạng thái nào. Không có số liệu nào ở trên do một service do chúng ta viết tính ra: [ksqlDB](https://ksqldb.io) đọc các topic Avro và duy trì các bảng trong [ksql/statements.sql](../ksql/statements.sql), còn dashboard chỉ chạy các pull query lên chúng (qua REST API của ksqlDB).

| Bảng | Dựng từ | Hiển thị gì |
|---|---|---|
| `orders_per_minute` | `order.created` | số đơn theo loại xe, trong các cửa sổ tumbling một phút |
| `match_stats` | join của `order.created` và `order.matched` | số lần ghép, thời gian trung bình và tối đa để tìm tài xế, theo loại xe |
| `matched_total`, `no_driver_total` | `order.matched`, `order.no_driver_found` | bao nhiêu đơn tìm được tài xế |
| `trip_status_totals` | `trip.status.changed` | số chuyến theo trạng thái (hoàn thành so với bị huỷ) |
| `driver_rating_stats` | `trip.rated` | số sao trung bình theo tài xế |
| `location_updates_per_minute` | `driver.location.updated` | thông lượng của topic bận nhất |

`driver_rating_stats` tính cùng giá trị trung bình như Rating Service tính trong Postgres: một bên là câu truy vấn khai báo trên topic, bên kia là một consumer do chúng ta tự viết và tự khử trùng lặp. Hãy so sánh chúng, và để ý ksqlDB cho bạn miễn phí điều gì (không cần code consumer, phát lại được từ topic) so với điều gì nó không cho (kết quả nằm trong ksqlDB, không phải trong một database mà các service khác đọc).

Tự thử, từ ksqlDB CLI (hoặc mục **KSQL** của Kafka UI):

```bash
docker run --rm -it --network grab-mini_default confluentinc/ksqldb-cli:0.29.0 ksql http://ksqldb-server:8088
```
```sql
SELECT * FROM match_stats;                                   -- a pull query: the current value
SELECT ORDERID, VEHICLETYPE FROM orders_created EMIT CHANGES;  -- a push query: streams new orders as they arrive
SHOW QUERIES;                                                -- every table above is a running query
```

ksqlDB suy ra tên cột từ schema Avro và viết hoa chúng (`orderId` thành `ORDERID`). Các bảng được dựng lại từ topic Kafka, nên chúng sống sót qua `docker compose down`.

## Những thứ nên thử

- **Scale một consumer group.** Khởi động một matching service thứ hai ở terminal khác và xem Kafka chia 3 partition giữa hai instance, không có đơn nào bị ghép hai lần:
  ```bash
  cd services/matching-service && APP_ENV=development INSTANCE_ID=matching-B go run .
  ```
  Tab **Consumers** trong Kafka UI cho thấy việc gán partition thay đổi.
- **Gửi một message không phải Avro** và xem nó rơi vào dead-letter topic. Một message JSON publish bằng tay không có header Schema Registry, nên Driver Location Service không giải mã được. Lỗi giải mã là vĩnh viễn, nên nó bỏ qua các lần thử lại và publish thẳng sang `driver.location.updated.DLQ`, kèm byte gốc (`valueBase64`, cộng với `valueText` đọc được) và lỗi. (Lỗi tạm thời, như Redis đang chết, được thử lại 3 lần, cách nhau 2 giây, trước khi chuyển vào dead-letter.)
  ```bash
  docker exec grab-mini-kafka bash -c 'echo "{\"driverId\":\"driver-99\"}" | /opt/kafka/bin/kafka-console-producer.sh --topic driver.location.updated --bootstrap-server localhost:9092'
  docker exec grab-mini-kafka /opt/kafka/bin/kafka-console-consumer.sh --topic driver.location.updated.DLQ --bootstrap-server localhost:9092 --from-beginning
  ```
- **Đọc về một schema đã tiến hoá.** Loại xe được thêm sau bản phát hành đầu tiên bằng cách cho `order.created` và `order.matched` một trường `vehicleType` có mặc định `CAR`, là một thay đổi tương thích ngược. Một lần khởi động mới chỉ đăng ký phiên bản hiện tại (`curl localhost:8081/subjects/order.created-value/versions`), nhưng test `TestCurrentOrderCreatedReadsMessagesWrittenBeforeVehicleType` trong [services/pkg/eventcodec](../services/pkg/eventcodec/eventcodec_test.go) cho thấy hiệu ứng: một message ghi bằng schema cũ vẫn giải mã được, với `vehicleType` đặt về giá trị mặc định.
- **Thử đăng ký một schema không tương thích.** Schema Registry chạy ở chế độ `BACKWARD`: một schema mới phải đọc được dữ liệu ghi bằng schema cũ. Thêm một trường *không có* mặc định phá điều đó, nên nó bị từ chối (HTTP 409); có mặc định thì ổn:
  ```bash
  # rejected: "vehicle" has no default
  curl -s -X POST localhost:8081/subjects/order.matched-value/versions -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
    -d '{"schema":"{\"type\":\"record\",\"name\":\"OrderMatched\",\"namespace\":\"grabmini.events\",\"fields\":[{\"name\":\"eventId\",\"type\":\"string\"},{\"name\":\"occurredAt\",\"type\":\"string\"},{\"name\":\"orderId\",\"type\":\"string\"},{\"name\":\"driverId\",\"type\":\"string\"},{\"name\":\"vehicle\",\"type\":\"string\"}]}"}'
  ```
- **Phát lại một topic mà không có gì bị đếm hai lần.** Đánh giá được lấy trung bình theo tài xế từ `trip.rated`, khử trùng lặp theo `eventId` trong cùng transaction với bản cập nhật. Dừng Rating Service, tua lại consumer group của nó, khởi động lại, và các event được đọc lại nhưng điểm trung bình không đổi (`duplicate trip.rated event ..., skipping` trong log của nó):
  ```bash
  pkill -f .dev/bin/rating-service   # then wait a few seconds for the group to go idle
  docker exec grab-mini-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
    --group rating-service-group --reset-offsets --to-earliest --topic trip.rated --execute
  cd services/rating-service && APP_ENV=development go run .
  ```
- **Phát lại một vị trí cũ** (US-3.1). Publish một `driver.location.updated` với `timestamp` trong quá khứ. Driver Location Service ghi `discarded as out-of-order` và vị trí của tài xế không nhúc nhích.
- **Làm mọi tài xế bận** rồi đặt xe, để thấy `order.no_driver_found`:
  ```bash
  for n in 1 2 3 4 5; do docker exec grab-mini-redis redis-cli SET "driver:driver-${n}:status" ON_TRIP; done
  ```
  Đặt họ về lại bằng `cd services/matching-service && go run . seed-drivers`. Hãy đặt key trong dấu nháy hoặc dùng `${n}`: trong zsh, `$n:status` không có ngoặc nhọn bị hiểu như một modifier và ghi sai key.

## Các service

| Service | Cổng | Consume | Produce | Trạng thái |
|---|---|---|---|---|
| Order Service | 3001 | `order.matched`, `order.no_driver_found` (để giữ trạng thái đơn luôn mới) | `order.created` | Postgres |
| Matching Service | | `order.created`, `order.cancelled`, `driver.application.reviewed` (ai được đề nghị chuyến) | `order.matched`, `order.no_driver_found` | Redis (trạng thái tài xế + GEO; `driver:<id>:suspended` cho một tài xế bị admin đưa ra khỏi hoạt động; [production-design §20](../specs/ride-booking-mini/production-design.md)) |
| Trip Service | 3002 | `order.matched` | `trip.status.changed`, `trip.rated` | Postgres, Redis (giải phóng tài xế) |
| Driver Location Service | | `driver.location.updated` | | Redis GEO |
| Rating Service | 3004 | `trip.rated` | | Postgres (tổng theo tài xế) |
| ksqlDB | 8088 | mọi topic ở trên | các bảng analytics (mục `analytics`) | Kafka (changelog topic) |
| Notification Service | 3003 | tất cả những cái trên | | Kết nối WebSocket (theo từng instance), Redis (trạng thái định tuyến và Pub/Sub giữa các instance) |
| Auth Service | 3005 | | | không có (bộ phát token development) |
| Pricing Service | 3007 | `order.created` (nhu cầu theo từng ô điểm đón) | | Redis (hệ số surge, bộ đếm nhu cầu); báo giá được ký, không lưu; hỏi Routing Service quãng đường (`ROUTING_URL`), lùi về đường thẳng nếu không được |
| Routing Service | 3009 | | | không có (stateless; mô hình xác định cho quãng đường và thời gian đi, không có dữ liệu bản đồ: [production-design §17](../specs/ride-booking-mini/production-design.md)) |
| Promotion Service | 3010 | `order.created`, `order.cancelled` (để ghi nhận và giải phóng các lần dùng mã) | `promotion.redeemed` | Postgres (khuyến mãi, lượt dùng, outbox); bước validate ký một `promoToken` ngắn hạn mà Order Service kiểm tra mà không phải gọi nó ([production-design §18](../specs/ride-booking-mini/production-design.md)) |
| Chat Service | 3011 | `trip.status.changed` (ai đang trên chuyến, và khi nào nó kết thúc), hoặc tra cứu ở Trip Service khi bị hụt | `chat.message.sent` | Postgres (chat_trips, chat_messages, outbox); tin nhắn là dữ liệu cá nhân, không bao giờ được log, bị xoá sau `CHAT_RETENTION_DAYS` ([production-design §19](../specs/ride-booking-mini/production-design.md)) |
| Profile Service | 3012 | | `driver.application.reviewed` | Postgres (rider_profiles, driver_profiles, outbox); tên, số điện thoại và biển số là dữ liệu cá nhân, không bao giờ được log ([production-design §20](../specs/ride-booking-mini/production-design.md)) |
| Payment Service | 3008 | `order.created` (giá và giảm giá đã khoá), `trip.status.changed`, `trip.rated` | `payment.recorded` | Postgres (sổ cái ghi kép, thanh toán); một `PaymentProvider` sandbox, không có bộ xử lý thẻ thật ([production-design §16](../specs/ride-booking-mini/production-design.md)) |
| Location Gateway | 3006 | | `driver.location.updated` | giới hạn tốc độ theo từng tài xế, trong bộ nhớ |
| `driver-app-sim` | | | | báo vị trí cho Location Gateway qua HTTP |

Order Service và Trip Service không bao giờ publish thẳng từ một request: event được lưu vào bảng `outbox` trong cùng transaction Postgres với đơn, trạng thái chuyến hoặc đánh giá, rồi một relay ([services/pkg/outbox](../services/pkg/outbox)) publish nó lên Kafka sau đó, nên một lần crash không thể để lại một thay đổi trạng thái mà không ai được báo. Matching Service ghi lại tài xế nào nó đã đưa cho mỗi đơn (`match:<orderId>` trong Redis), nên một `order.created` bị gửi lại không thể chiếm tài xế thứ hai, và một tài xế đã được giữ chỉ được *cho mượn* (`ASSIGNED` trong `ASSIGN_LEASE_SECONDS`, mặc định 120) cho đến khi Trip Service xác nhận chuyến và đặt họ thành `ON_TRIP`. Trip Service cũng giải phóng tài xế từ event `trip.status.changed` (một lưới an toàn cho trường hợp crash giữa lúc lưu chuyến và cập nhật Redis). Mọi service dừng sạch khi nhận SIGTERM / Ctrl-C.

Một message mà consumer chưa xử lý được lúc này (database đang chết) không được thử lại tại chỗ: nó chuyển sang `<topic>.retry.1`, rồi `.2`, `.3` (chờ 5 giây, 1 phút, 10 phút), để partition vẫn chảy; sau tầng cuối, hoặc ngay lập tức với một message không bao giờ thành công, nó rơi vào `<topic>.DLQ`. Notification Service có thể chạy thành nhiều instance: event được bất kỳ instance nào consume, và mỗi message được publish lên một kênh Redis dành cho người nhận, để instance đang giữ socket đó chuyển nó đi.

### Bảo mật

Mọi lời gọi API và mọi WebSocket đều mang một JWT, và các service tin token, không tin request: id của rider lấy từ token của họ (một body nêu tên người khác là 403), một rider chỉ thấy đơn và chuyến của chính mình, chỉ tài xế của chuyến mới đón khách hay hoàn thành nó, và chỉ rider của nó mới đánh giá được. Token là JWT Ed25519 (EdDSA): chỉ bên phát giữ khoá riêng và các service xác minh bằng khoá công khai của nó từ `GET /.well-known/jwks.json` (`JWT_JWKS_URL`), nên không service nào tự phát được token; xem [security.md](security.md). Ở development (`APP_ENV=development`, mà `scripts/dev.sh` đặt) [Auth Service](../services/auth-service) ký một token cho bất kỳ ai hỏi (`curl -X POST localhost:3005/token -d '{"role":"rider","id":"rider-1"}'`; role là `rider`, `driver` hoặc `admin`, loại cuối cho Ops dashboard của người vận hành và các route chỉ dành cho admin), vì vậy nó từ chối khởi động với `APP_ENV=production`; các app đăng nhập qua `@grab-mini/client`. Một WebSocket gửi token dưới dạng subprotocol (không nằm trong URL) và chỉ được chấp nhận từ một origin nằm trong `ALLOWED_ORIGINS`.

Điện thoại của tài xế không bao giờ nói chuyện với Kafka. [Location Gateway](../services/location-gateway) xác thực tài xế, kiểm tra vị trí, giới hạn mỗi tài xế một báo cáo mỗi giây, bỏ qua các báo cáo của một tài xế đang đỗ, và publish theo lô.

Credential đến từ `NAME_FILE` (một secret được mount) hoặc `NAME`, và với `APP_ENV=production` thiếu một cái là lỗi thay vì dùng mặc định của development: `DATABASE_URL`, `KAFKA_PASSWORD`, và (chỉ cho bên phát) `JWT_PRIVATE_KEY`; bên xác minh cần `JWT_JWKS_URL` hoặc `JWT_PUBLIC_KEY_FILE`. Các khoá ký, `JWT_PRIVATE_KEY` và `QUOTE_SECRET`, nghiêm hơn: giá trị development đã công khai của chúng chỉ được dùng với `APP_ENV=development` (để một deployment quên `APP_ENV` sẽ thất bại thay vì chạy với chúng), và bị từ chối ở mọi nơi khác kể cả khi đặt tường minh; một bên xác minh cũng từ chối tin khoá công khai development ngoài development. Schema Registry nhận `SCHEMA_REGISTRY_USER` / `SCHEMA_REGISTRY_PASSWORD` (hoặc `_FILE`). Với Kafka, đặt `KAFKA_SECURITY_PROTOCOL=SASL_SSL`, `KAFKA_USERNAME`, `KAFKA_PASSWORD_FILE` và `KAFKA_CA_FILE` (xem [pkg/kafkaconn](../services/pkg/kafkaconn)); production từ chối kết nối plaintext. Mỗi service có principal riêng và `scripts/kafka-acls.sh` chỉ cấp cho nó đúng những gì nó làm (bảng trong script đó là nguồn sự thật). Để thử một Kafka có TLS, SASL/SCRAM và ACL mà không cần Docker:

```bash
KAFKA_HOME=/path/to/kafka_2.13-3.8.0 deploy/kafka-secure/start-local.sh   # prints the environment to give a service
```

`docker compose` vẫn khởi động một Kafka PLAINTEXT không xác thực cho development.

Mỗi service sở hữu schema của mình dưới dạng SQL có phiên bản trong `services/<service>/migrations` (goose, một bảng version cho mỗi service). Chúng được áp dụng khi khởi động; `<service> migrate` áp dụng chúng rồi thoát, và `RUN_MIGRATIONS=false` bỏ qua chúng khi khởi động. Mọi service có HTTP API đều phục vụ `/livez` (còn sống) và `/readyz` (Postgres, Redis và Kafka của nó có trả lời; 503 khi quá trình tắt bắt đầu); Matching và Driver Location không có API, nên đặt `HEALTH_PORT` để có cùng hai probe.

Mọi consumer chạy qua [services/pkg/kafkaconsumer](../services/pkg/kafkaconsumer): thử lại, rồi dead-letter topic, và log đồng nhất topic/partition/offset. Mọi producer và consumer mã hoá và giải mã qua [services/pkg/eventcodec](../services/pkg/eventcodec) (Avro cộng định dạng wire của Schema Registry); các schema nằm ở [services/pkg/events/schemas](../services/pkg/events/schemas). Event trùng lặp là vô hại (khử trùng theo `eventId`, chuyển trạng thái idempotent).

`driver-app-sim` publish vị trí của các tài xế vì trình duyệt không thể produce thẳng vào Kafka; Driver App chỉ hiển thị vị trí của nó.

Để chạy từng phần bằng tay thay cho `pnpm dev:all` (khi `docker compose up -d` đã chạy):

```bash
export APP_ENV=development                              # lets the services use the public development signing keys
cd services/matching-service && go run . seed-drivers   # once, to seed drivers
cd services/order-service && go run .                   # likewise for each service
```

Không có `APP_ENV=development` (hoặc một `JWT_PRIVATE_KEY` thật cho bên phát, `JWT_JWKS_URL` hoặc `JWT_PUBLIC_KEY_FILE` cho các service kiểm tra token, và một `QUOTE_SECRET` thật), các service ký hoặc kiểm tra token và báo giá sẽ từ chối khởi động: khoá development nằm ngay trong repository này, nên chúng không bao giờ bị dùng nhầm.

### HTTP API

```bash
# Order Service
curl -X POST localhost:3001/orders -H "Content-Type: application/json" \
  -d '{"riderId": "rider-1", "pickup": {"lat": 10.776, "lng": 106.700}, "dropoff": {"lat": 10.79, "lng": 106.71}, "vehicleType": "BIKE"}'   # vehicleType: CAR (default) or BIKE
curl localhost:3001/orders/<orderId>                  # includes status: PENDING, then MATCHED or NO_DRIVER
curl localhost:3001/riders/<riderId>/latest-order     # what a reloaded Rider App uses to find its ride

# Trip Service
curl localhost:3002/trips/by-order/<orderId>
curl localhost:3002/trips/<tripId>                    # a COMPLETED trip also has completedAt (RFC 3339), and receiptClosedAt once the rider closed or rated it; both empty for the others
curl localhost:3002/drivers/<driverId>/active-trip    # the driver's ASSIGNED or IN_PROGRESS trip, else 404
curl -X POST localhost:3002/trips/<tripId>/pickup     # ASSIGNED -> IN_PROGRESS
curl -X POST localhost:3002/trips/<tripId>/complete   # IN_PROGRESS -> COMPLETED (answers with completedAt), driver becomes AVAILABLE
curl -X POST localhost:3002/trips/<tripId>/receipt/close   # rider token; COMPLETED only (409 not_completed); repeat keeps the first receiptClosedAt; rating closes it too
curl -X POST localhost:3002/trips/<tripId>/cancel     # from ASSIGNED or IN_PROGRESS; 409 once COMPLETED
curl -X POST localhost:3002/trips/<tripId>/rating -H "Content-Type: application/json" \
  -d '{"riderId": "rider-1", "stars": 5, "comment": "Smooth ride", "tipAmount": 20000}'   # COMPLETED trips only, once (409 otherwise); tipAmount is optional whole VND, 0 to 1,000,000

# Rating Service
curl localhost:3004/drivers/driver-1/rating          # {"average": 4.67, "count": 3, "totalTips": 30000, "tipCount": 2}
curl "localhost:3004/drivers/driver-1/reviews?limit=20&before=<createdAt>"   # a driver's own reviews, newest first (driver token only)

# Routing Service (any valid token: rider, driver or admin). Distance by road (great-circle x 1.3) and travel time for the hour in Vietnam
curl "localhost:3009/route?fromLat=10.776&fromLng=106.700&toLat=10.790&toLng=106.710&vehicle=BIKE" -H "Authorization: Bearer <token>"   # {"distanceKm": 2.47, "durationSec": 405, "polyline": [{"lat":...,"lng":...}, ...], "vehicle": "BIKE", "model": "detour-1.3"}; vehicle is CAR (default) or BIKE; 400 for bad input, 422 beyond 200 km

# Promotion Service. Riders check a code for a price (whole VND) and vehicle; admins manage promotions (an admin token: {"role":"admin","id":"ops-1"})
curl "localhost:3010/promotions/validate?code=WELCOME10&price=87000&vehicleType=CAR" -H "Authorization: Bearer <rider token>"   # {"code":"WELCOME10","discount":8700,"price":87000,"finalPrice":78300,"promoToken":"...","expiresAt":...}; otherwise 404/422 {"error":"...","code":"not_found|not_started|expired|min_fare|wrong_vehicle|limit_reached|already_used"}
curl -X POST localhost:3001/orders -H "Authorization: Bearer <rider token>" -H "Content-Type: application/json" \
  -d '{"pickup":{"lat":10.776,"lng":106.700},"dropoff":{"lat":10.790,"lng":106.710},"promoToken":"<promoToken>"}'   # locks the discount: {"orderId":...,"price":87000,"discount":8700,"finalPrice":78300,"promoCode":"WELCOME10"}; a stale or foreign token is 400 {"code":"promo_expired|promo_wrong_price|promo_wrong_rider|promo_wrong_vehicle|promo_invalid"}, a used one 409 promo_used
curl localhost:3010/promotions -H "Authorization: Bearer <admin token>"   # every promotion with how often it was redeemed (redeemed, overLimit)
curl -X POST localhost:3010/promotions -H "Authorization: Bearer <admin token>" -H "Content-Type: application/json" \
  -d '{"code":"SUMMER20","kind":"PERCENT","value":20,"maxDiscount":30000,"minFare":50000,"vehicleType":"CAR","validTo":"2027-01-31T00:00:00Z","usageLimit":100,"perRiderLimit":2}'   # kind PERCENT or FIXED; only code, kind, value and validTo are required
curl -X POST localhost:3010/promotions/SUMMER20/disable -H "Authorization: Bearer <admin token>"   # stops new uses; orders that carry its discount keep it

# Chat Service (rider or driver token; only the rider and the driver of that trip. The chat opens when the trip is assigned and stays writable for 10 minutes after it ends)
curl -X POST localhost:3011/trips/<tripId>/messages -H "Authorization: Bearer <rider or driver token>" -H "Content-Type: application/json" -H "Idempotency-Key: <any unique string>" \
  -d '{"text":"I am at the gate"}'   # 201 {"id":...,"tripId":...,"senderId":...,"senderRole":"rider","text":"I am at the gate","createdAt":...}; 1 to 500 characters, no control characters (400 invalid_text); 403 not_participant, 404 unknown_trip, 503 trip_lookup_unavailable (Trip Service could not be asked about a trip chat had not learned yet), 409 chat_closed, 429 rate_limited (Retry-After); the same Idempotency-Key and text again returns the saved message (200) instead of sending twice
curl "localhost:3011/trips/<tripId>/messages?limit=20" -H "Authorization: Bearer <rider or driver token>"   # {"messages":[newest first],"nextBefore":"...","tripStatus":"ASSIGNED","open":true,"closesAt":null}; pass nextBefore as ?before= for older ones. The other person gets a "chat.message.sent" push on their WebSocket

# Profile Service (rider and driver tokens act on their own id only; a different id is 403). `scripts/dev.sh` seeds approved profiles for driver-1..5 and profiles for rider-1..3
curl -X PUT localhost:3012/riders/rider-1 -H "Authorization: Bearer <rider token>" -H "Content-Type: application/json" \
  -d '{"name":"Hoàng Gia Bảo","phone":"0900 100 001"}'   # name 1 to 80 characters (required); phone optional, 7 to 15 digits; 400 {"code":"invalid_profile","field":"phone"}; GET /riders/rider-1 reads it back (empty, not 404, before the first save)
curl -X PUT localhost:3012/drivers/driver-9 -H "Authorization: Bearer <driver token>" -H "Content-Type: application/json" \
  -d '{"name":"Lê Minh","phone":"0900 000 009","vehicleType":"BIKE","plate":"59-x1 123.45","model":"Honda Wave","color":"Xanh"}'   # saves a draft (status NONE); the plate is stored in capitals. Changing the plate or vehicle type of an APPROVED driver puts them back to PENDING and out of service until an admin approves
curl -X POST localhost:3012/drivers/driver-9/application -H "Authorization: Bearer <driver token>"   # 201 status PENDING; again, or when APPROVED: 200 unchanged; 409 suspended; 422 incomplete_profile (name, vehicleType and plate are needed)
curl "localhost:3012/driver-applications?status=PENDING&limit=20" -H "Authorization: Bearer <admin token>"   # {"applications":[oldest first, with phone],"nextCursor":"..."}; pass nextCursor as ?cursor=
curl -X POST localhost:3012/drivers/driver-9/approve -H "Authorization: Bearer <admin token>"   # PENDING or SUSPENDED to APPROVED; Matching then makes the driver AVAILABLE (if not on a trip). The driver must also send locations
curl -X POST localhost:3012/drivers/driver-9/suspend -H "Authorization: Bearer <admin token>" -H "Content-Type: application/json" -d '{"reason":"Complaint 4412"}'   # APPROVED to SUSPENDED; reject (from PENDING) and suspend need a reason of 1 to 500 characters; any other transition is 409 invalid_transition with the current status. A driver on a trip finishes it and is then offered no more
curl -X POST localhost:3012/driver-applications/resync -H "Authorization: Bearer <admin token>"   # after Redis lost its data: re-publishes the current status of every APPROVED, REJECTED or SUSPENDED driver so Matching rebuilds eligibility; {"queued":3,"counts":{"APPROVED":2,"REJECTED":0,"SUSPENDED":1},"skipped":0}. Add -H "Content-Type: application/json" -d '{"driverId":"driver-9"}' for one driver; 409 resync_running while another runs; without a token: profile-service resync [--driver driver-9]
curl localhost:3012/drivers/driver-1/public -H "Authorization: Bearer <any token>"   # {"driverId":...,"name":...,"vehicleType":"CAR","plate":"51A-123.45","model":...,"color":...} for an APPROVED driver, never the phone; 404 for anyone else

# Payment Service (tokens as above; a rider sees what they were charged, a driver what they earn)
curl "localhost:3008/payments?tripId=<tripId>" -H "Authorization: Bearer <token>"   # fare, tip and cancellation fee of one trip, plus the captured total
curl localhost:3008/cancellation-policy -H "Authorization: Bearer <token>"   # {"currency":"VND","fee":10000,"graceSeconds":180}: what cancelling after a driver is assigned costs, and how long it is free
curl localhost:3008/drivers/driver-1/earnings -H "Authorization: Bearer <driver token>"   # {"total": 98000, "fares": 80000, "tips": 18000, "commission": 20000, "trips": 1, ...}; optional ?from=&to= (RFC 3339)
```

(Các comment trong khối lệnh trên giữ nguyên tiếng Anh như bản gốc, vì chúng mô tả đúng từng mã lỗi và trường JSON của API.)

Lặp lại `pickup` hoặc `complete` là một no-op. WebSocket là `ws://localhost:3003/ws/rider/<id>` hoặc `/ws/driver/<id>`, đẩy `{"type": "...", "payload": {...}}`.

Xem trạng thái trực tiếp:

```bash
docker exec grab-mini-redis redis-cli GEOPOS drivers:geo driver-1
docker exec grab-mini-redis redis-cli GET "driver:driver-1:status"
docker exec grab-mini-redis redis-cli GET "driver:driver-1:vehicle"
docker exec grab-mini-postgres psql -U grabmini -d grabmini -c "select * from trips;"
```

**Thử lại (retry).** `POST /orders` nhận một header `Idempotency-Key` tuỳ chọn (1 đến 128 ký tự in được; `@grab-mini/client` gửi một UUID mới cho mỗi lần đặt xe và dùng lại nó khi thử lại sau lỗi mạng hoặc 5xx). Lặp lại một key với cùng body trả về response ban đầu (cùng `orderId`, header `Idempotent-Replayed: true`) và không publish gì; cùng key với body khác là `422`. Key được gắn theo rider và giữ 24 giờ. Một `quoteId` chỉ đặt được một lần: dùng lại là `409` kèm một `newQuote` mới (nếu Redis chết thì lần đặt được tính giá theo hệ số hiện tại thay vì bị từ chối).

**Pickup và complete cần tài xế có mặt.** `pickup` bị từ chối với `409` trừ khi vị trí báo cáo gần nhất của tài xế nằm trong `PICKUP_RADIUS_M` (mặc định 200) quanh điểm đón, còn `complete` trong `DROPOFF_RADIUS_M` (mặc định 300) quanh điểm trả. `code` của lỗi cho biết lý do: `driver_too_far`, `driver_location_unknown`, hoặc `driver_location_stale` (cũ hơn `LOCATION_MAX_AGE`, mặc định 60 giây); nếu không kết nối được Redis thì là `503`. Lặp lại một bước đã xảy ra vẫn là no-op. `driver-app-sim` chỉ đi lang thang quanh điểm seed của nó, nên `make dev` và `deploy/loadtest/stack.sh` khởi động Trip Service với `APP_ENV=development PROXIMITY_CHECK=off`; Trip Service từ chối `PROXIMITY_CHECK=off` dưới bất kỳ `APP_ENV` nào khác. Để thử việc kiểm tra này ở máy local, bỏ đặt biến đó và báo một vị trí trước (`POST localhost:3006/locations` với token tài xế) hoặc dùng `redis-cli`/`driverlocation.Update`.

## Cấu trúc repository

**Analytics**: [ksql/statements.sql](../ksql/statements.sql), các stream và bảng ksqlDB, được áp dụng bởi container `ksql-init`.

**Backend**, một Go workspace ([go.work](../go.work)) với một module cho mỗi service:
- `services/*-service` (order, matching, trip, driver-location, notification, rating), `services/driver-app-sim`
- `services/pkg/events`: kiểu event Kafka, tên topic và các schema Avro (`schemas/*.avsc`)
- `services/pkg/eventcodec`: mã hoá/giải mã Avro với Schema Registry (đăng ký schema, xử lý schema của writer so với reader)
- `services/pkg/kafkaconsumer`: vòng lặp consumer có thử lại + dead-letter
- `services/pkg/driverstatus`: các key Redis cho trạng thái và loại xe của tài xế, và thao tác chiếm nguyên tử (AVAILABLE và đúng loại xe, hoặc không có gì)
- `services/pkg/driverlocation`: key GEO của Redis và bản cập nhật vị trí an toàn với thứ tự đảo

**Frontend**, một pnpm workspace (React, TypeScript, Vite):
- `apps/rider`, `apps/driver`: hai app, và `apps/ops`: dashboard analytics (đọc ksqlDB)
- `packages/ui`: design system, với token (biến CSS, sáng và tối) và component
- `packages/client`: các lời gọi REST và hook WebSocket thông báo
- `packages/events`: kiểu cho các message WebSocket

Khám phá design system riêng lẻ bằng `pnpm dev:ui` (http://localhost:5170). `pnpm --filter @grab-mini/ui build` tạo bản build thư viện (ES module, `style.css`, khai báo kiểu); bên trong workspace, các app import nó từ source.

Ở dev, các app đến các service Go qua proxy của Vite, vì các service không gửi header CORS.

## Công cụ tự động hoá & Makefile

Dự án có một [Makefile](../Makefile) đầy đủ cho phát triển, hạ tầng, kiểm thử và vận hành. Chạy `make help` (hoặc `make`) để xem mọi target:

```bash
make help
```

### Phát triển & Build
- `make dev`: Khởi động toàn bộ môi trường (hạ tầng Docker, stack Observability, các service Go, bộ giả lập tài xế, các app web).
- `make dev-reset`: Xoá các Docker volume và đặt lại mọi thứ từ đầu (`scripts/dev.sh --reset`).
- `make build`: Biên dịch mọi binary Go vào `.dev/bin` và build mọi ứng dụng frontend.
- `make seed`: Seed 5 tài xế mặc định (ô tô và xe máy) vào Redis.

### Hạ tầng & Docker
- `make infra-up`: Khởi động mọi container Docker (Kafka, Postgres, Redis, Schema Registry, ksqlDB, Prometheus, Loki, Promtail, Grafana).
- `make infra-down`: Dừng mọi container Docker (giữ lại volume).
- `make infra-reset`: Dừng mọi container Docker và xoá volume.
- `make infra-ps`: Liệt kê trạng thái và độ khoẻ của mọi container Docker.
- `make obs-up`: Chỉ khởi động stack Observability (`prometheus`, `loki`, `promtail`, `grafana`).

### CLI cho Log & Metrics
- `make logs`: Tail mọi log của service trong `.dev/logs/*.log`.
- `make logs SVC=order-service`: Tail log của một service cụ thể.
- `make logs-errors`: Lọc mọi dòng `WARN` và `ERROR` trong `.dev/logs/*.log`.
- `make logs-loki SVC=matching-service LIMIT=50`: Truy vấn log gần đây trực tiếp từ Loki qua HTTP API.
- `make metrics-check`: Thăm dò `/readyz`, đếm các series `/metrics` đang hoạt động trên cả 9 service Go, và liệt kê các scrape target đang hoạt động của Prometheus.
- `make prom-check`: Kiểm tra cấu hình Prometheus (`prometheus.yml`), các alert rule (`alerts.rules.yaml`), và chạy unit test của rule (`alerts.test.yaml`).

### Chất lượng, Kiểm thử & Vận hành
- `make test`: Chạy unit test trên mọi module Go trong `go.work`.
- `make test-integration`: Chạy các test Go cần Postgres, Redis và Kafka thật.
- `make fmt`: Format toàn bộ code Go (`gofmt -s -w`).
- `make lint`: Chạy kiểm tra format, `go vet`, và `typecheck` TypeScript.
- `make verify`: Chạy [deploy/verify.sh](../deploy/verify.sh) (manifest K8s, CRD, alert rule, static build).
- `make migrate`: Chạy migration schema SQL cho `order-service`, `trip-service` và `rating-service`.
- `make kafka-topics`: Liệt kê mọi topic Kafka trong cluster.
- `make kafka-lag`: Mô tả offset và lag của consumer group trên mọi service.
- `make dlq-dry-run TOPIC=order.created`: Xem các message dead-letter mà không redrive.
- `make dlq-redrive TOPIC=order.created`: Redrive các message dead-letter về topic nguồn của chúng.
- `make loadtest RATE=50 DURATION=30s DRIVERS=100`: Chạy bộ giả lập tải end-to-end.
- `make clean`: Xoá `.dev/bin`, `.dev/logs` và các output build `dist` của frontend.

## Observability (Metrics & Logging)

Stack local gồm một bộ observability đầy đủ chạy qua Docker Compose:

1. **Grafana (http://localhost:3000)**:
   - Đăng nhập bằng `admin` (mật khẩu `admin`, hoặc `GRAFANA_ADMIN_PASSWORD` nếu đã đặt khi khởi động Compose). Truy cập ẩn danh bị tắt.
   - Dashboard dựng sẵn: **`grab-mini — Observability (Metrics & Logs)`**, gồm:
     - **Service Health & HTTP Traffic**: trạng thái Up/Down, tốc độ request HTTP theo route & code, độ trễ p50/p95.
     - **Business Metrics**: tốc độ tạo đơn, kết quả ghép, độ trễ ghép so với SLO 3 giây, chuyển trạng thái chuyến, đánh giá, WebSocket đang mở.
     - **Kafka Streaming & Outbox**: thông lượng consumer (ok / retried / dead_lettered), tuổi của outbox đang chờ, độ trễ publish, hệ số surge.
     - **Centralized Logs (Loki)**: luồng log trực tiếp và khối lượng log theo thời gian.
   - Khám phá log tương tác trong Grafana Explore bằng LogQL:
     - Theo service: `{job="grab-mini", service="order-service"}`
     - Theo mức độ: `{job="grab-mini", level="ERROR"}`
     - Theo distributed trace: `{job="grab-mini"} |= "<trace_id>"`
2. **Prometheus (http://localhost:9090)**:
   - Scrape `/metrics` mỗi 5 giây từ mọi service Go (qua `host.docker.internal`).
   - Đánh giá các alert rule từ [deploy/monitoring/alerts.rules.yaml](../deploy/monitoring/alerts.rules.yaml) (xem trạng thái ở http://localhost:9090/alerts).
3. **Loki (http://localhost:3100) & Promtail**:
   - Promtail tail `.dev/logs/*.log`, tự trích `service` từ tên file, và phân tích các thuộc tính JSON `slog` có cấu trúc (`level`, `service`, `trace_id`, `span_id`).

## Vận hành

Mỗi service log JSON ở production (kèm `trace_id` trên các dòng nằm trong một span) và phục vụ metric Prometheus; một request giữ một trace xuyên suốt HTTP, outbox, Kafka và các retry topic. `deploy/` có một Dockerfile, một Kustomize base (Kubernetes, Kafka Strimzi trải ba zone, KEDA scale theo consumer lag, alert) và `deploy/verify.sh` để kiểm tra nó mà không cần cluster; [docs/runbook.md](runbook.md) nói phải làm gì khi một alert nổ, [docs/disaster-recovery.md](disaster-recovery.md) nói về backup và failover, và `deploy/loadtest` đo thông lượng mà không cần Docker ([kết quả và ý nghĩa](performance.md)). `dlq-redrive` gửi dead letter về topic của chúng khi nguyên nhân đã được sửa:

```bash
KAFKA_BROKERS=localhost:9092 go run ./services/dlq-redrive -topic order.created -dry-run
# or simply:
make dlq-dry-run TOPIC=order.created
```

## Chạy các công cụ ở máy local

### Design System Playground
```bash
pnpm dev:ui
```
Mở bộ khám phá component UI và design token tại http://localhost:5170.

### Load test end-to-end
```bash
# Run against the running `make dev` stack:
make loadtest RATE=30 DURATION=30s DRIVERS=50
```
Hoặc khởi động một stack Go cô lập, không dùng Docker, với `dev-registry` bằng [deploy/loadtest/stack.sh](../deploy/loadtest/stack.sh):
```bash
DATABASE_URL='postgres://grabmini:grabmini@localhost:5432/grabmini_load?sslmode=disable' deploy/loadtest/stack.sh start
go run ./services/loadtest -rate 50 -duration 30s -drivers 100
DATABASE_URL='postgres://grabmini:grabmini@localhost:5432/grabmini_load?sslmode=disable' deploy/loadtest/stack.sh stop
```

### Kafka bảo mật (TLS + SASL + ACL)
```bash
KAFKA_HOME=/path/to/kafka_2.13-3.8.0 deploy/kafka-secure/start-local.sh
```

## Test

```bash
# Run all Go unit tests + Frontend unit & component tests (Vitest)
make test

# Frontend unit & component tests only (Vitest + Testing Library)
pnpm test

# Browser E2E tests across Rider, Driver & Ops apps (Playwright)
pnpm test:e2e

# Go integration + full-stack system tests against local Postgres, Redis, Kafka
make test-integration

# Code quality and typecheck
make lint
```

Hầu hết test Go không cần gì cả. Một vài test (gồm test hệ thống toàn stack trong [services/e2e-test](../services/e2e-test)) chạy với Postgres, Redis và Kafka thật và tự bỏ qua trừ khi bạn trỏ chúng vào một instance dùng một lần: `TEST_DATABASE_URL=postgres://grabmini:grabmini@localhost:5432/grabmini_test` và `TEST_REDIS_ADDR=localhost:6379`. Các test của Kafka consumer, outbox relay, producer của Location Gateway, và `services/e2e-test` cũng cần một broker: `TEST_KAFKA_BROKERS=localhost:9092`. Các test bảo mật trong `services/pkg/kafkaconsumer` chạy với một broker được tạo bởi `deploy/kafka-secure/start-local.sh`.

## Xử lý sự cố

- **`the Docker daemon isn't running`**: khởi động Docker Desktop.
- **Một cổng đã bị chiếm** (3000-3009, 5170-5173, 5432, 6379, 8080, 8081, 8088, 9090, 9092, 9101, 9102): dừng thứ đang giữ nó. Vite âm thầm chuyển sang cổng kế tiếp nếu cổng của nó bị chiếm, điều này làm hỏng các URL ở trên; `lsof -nP -iTCP:<port> -sTCP:LISTEN` cho thấy thủ phạm.
- **Rider cứ kẹt ở "Looking for a driver"**: kiểm tra `.dev/logs/matching-service.log` và các tài xế đã được seed (`docker exec grab-mini-redis redis-cli GEOPOS drivers:geo driver-1`).
- **Rider app đang ở một màn hình không khớp với chuyến**: tải lại nó. Nó hỏi Order Service đơn mới nhất của rider và Trip Service chuyến của nó, rồi tiếp tục các chuyến đang tìm, đã gán hoặc đang đi (một push WebSocket bị lỡ khi đang tìm cũng được bắt lại bằng cách polling mỗi 3 giây).
- **Driver app không hiện chuyến nào**: kiểm tra id `?driver=` khớp với tài xế mà rider đã được ghép (thẻ của rider hiện nó). Một chuyến đang diễn ra được khôi phục khi trang mở hoặc tải lại.
- **Ops dashboard báo không kết nối được ksqlDB, hoặc không có dữ liệu**: ksqlDB là một JVM và mất một phút để khởi động; `docker logs grab-mini-ksqldb` cho thấy tiến độ của nó, và `docker logs grab-mini-ksql-init` cho thấy các bảng đã được tạo chưa. Các bảng đầy dần khi event chảy, nên hãy đặt một chuyến trước.
- **Bắt đầu lại từ đầu**: `make dev-reset` (hoặc `scripts/dev.sh --reset`).
