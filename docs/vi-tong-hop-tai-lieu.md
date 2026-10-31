# Tổng hợp tài liệu (tiếng Việt)

Toàn bộ tài liệu gốc của dự án viết bằng **tiếng Anh**. File này là bản đồ bằng tiếng Việt: mỗi file `.md` nói về điều gì, khi nào cần đọc, và nên đọc theo thứ tự nào. Nó chỉ tóm tắt, không thay thế các file gốc.

## 1. Đọc theo mục đích

| Bạn muốn | Đọc |
|---|---|
| Hiểu nhanh dự án là gì | [README.md](../README.md) |
| Chạy dự án và quan sát Kafka | [guide.md](guide.md) |
| Hiểu vì sao thiết kế như vậy (chuẩn bị phỏng vấn) | [adr/](adr/README.md), rồi [scaling.md](scaling.md) và [performance.md](performance.md) |
| Hiểu đặc tả gốc | [specs/ride-booking-mini/](../specs/ride-booking-mini) |
| Vận hành, xử lý sự cố | [runbook.md](runbook.md), [disaster-recovery.md](disaster-recovery.md) |
| Triển khai lên Kubernetes | [deploy/README.md](../deploy/README.md) |
| Bảo mật | [security.md](security.md) |

## 2. Thứ tự đọc đề xuất

1. `README.md`: tổng quan.
2. `docs/guide.md`: chạy và thử.
3. `docs/adr/README.md`, rồi lần lượt ADR 0001 đến 0010.
4. `docs/scaling.md` và `docs/performance.md`.
5. Đặc tả: `requirements.md` → `design.md` → `tasks.md` → `production-design.md`.
6. Vận hành và triển khai khi cần: `runbook.md`, `disaster-recovery.md`, `security.md`, `deploy/README.md`, `deploy/loadtest/README.md`.

## 3. Từng file

### Gốc dự án

**[README.md](../README.md)** (khoảng 63 dòng): trang chủ ngắn. Gồm mô tả dự án, sơ đồ kiến trúc, bảng "dự án thể hiện được gì", các số đo kèm giới hạn (đo trên một máy 4 nhân, chưa kiểm chứng trên hạ tầng thật), quick start, bảng chỉ đường tới tài liệu khác, phần "giới hạn thẳng thắn" và giấy phép MIT.

### Thư mục `docs/`

**[guide.md](guide.md)** (khoảng 366 dòng): hướng dẫn nhà phát triển, là README dài cũ đã chuyển vào đây. Nội dung: cài đặt và chạy bằng `pnpm dev:all`, các bước đặt thử một chuyến, cách xem Kafka trong Kafka UI, phân tích bằng ksqlDB, các thí nghiệm nên thử (replay topic, schema không tương thích), bảng service và cổng, bảo mật token, API HTTP, cấu trúc thư mục, các lệnh Makefile, quan sát (observability), vận hành, test và xử lý sự cố.

**[scaling.md](scaling.md)**: phân tích "khi tải tăng 10 lần thì cái gì vỡ trước". Xếp theo thứ tự: Redis GEO, Order Service và Postgres, outbox relay, số partition của Matching, Notification, Location Gateway, Kafka, các bảng dedupe phình dần. Mỗi mục nêu lý do, cần đo gì, nên làm gì. Nêu thẳng là chưa chạy trên hạ tầng thật nên đây là giả thuyết, không phải kết quả đo.

**[performance.md](performance.md)**: những gì đã đo trên một máy 4 nhân. Gồm 4 lỗi hiệu năng đã tìm ra và sửa (batch timeout của writer, commit từng message, matching xử lý tuần tự, quá nhiều lệnh Redis mỗi đơn), kết quả sau khi sửa (250 đơn/giây được match 100%), và phần phân tích Redis GEO cùng việc có cần shard hay không.

**[runbook.md](runbook.md)** (khoảng 410 dòng): sổ tay xử lý sự cố, theo từng alert của Prometheus. Các mục: cách điều tra một sự cố, consumer lag, dead letter, Kafka broker chết, outbox tồn đọng, matching chậm, lỗi đặt xe, surge pricing, Redis failover, sao lưu và khôi phục, Payments, pod chưa sẵn sàng, và các thao tác thường gặp.

**[disaster-recovery.md](disaster-recovery.md)**: sao lưu và khôi phục sau thảm họa. Gồm bảng "dữ liệu nào quan trọng đến đâu", Redis Sentinel (mất một node), Postgres, Kafka mirror sang vùng dự phòng bằng MirrorMaker2, lưu trữ lạnh trên S3, quy trình failover, và phần "chưa làm".

**[security.md](security.md)**: ghi chú bảo mật. Gồm chữ ký và xác minh token JWT bất đối xứng (Ed25519), xoay khóa, cách chuyển từ secret HS256 dùng chung, bảo mật Schema Registry, network policy, và bảo mật chuỗi cung ứng trên CI.

**`SECURITY-REVIEW.md`** (đã bị `.gitignore`, chỉ có trên máy bạn): báo cáo rà soát bảo mật ngày 2026-10-25 do AI soạn, chỉ đọc mã, không kiểm thử động. Chính file này ghi là cần người kiểm lại trước khi tin.

**[vi-tong-hop-tai-lieu.md](vi-tong-hop-tai-lieu.md)**: chính file này.

### Thư mục `docs/adr/` (ADR = bản ghi quyết định kiến trúc)

Mỗi ADR theo khung: Context (bối cảnh), Decision (quyết định), Alternatives (phương án khác), Consequences (hệ quả, cả tốt lẫn xấu), Revisit when (khi nào xem lại). Bạn nên đọc theo số thứ tự.

| File | Quyết định | Ý chính |
|---|---|---|
| [README.md](adr/README.md) | Mục lục | Bảng một dòng cho mỗi ADR |
| [0001](adr/0001-kafka-event-driven-architecture.md) | Kafka làm xương sống | Service giao tiếp bằng event, không gọi trực tiếp nhau, nên một consumer chậm không chặn đường đặt xe |
| [0002](adr/0002-partition-keys-and-ordering.md) | Partition key | Chọn key theo thực thể cần giữ thứ tự (`orderId`, `tripId`, `driverId`), không có thứ tự toàn cục |
| [0003](adr/0003-at-least-once-and-idempotent-consumers.md) | Ngữ nghĩa giao nhận | At-least-once cộng consumer idempotent, không dùng exactly-once của Kafka |
| [0004](adr/0004-transactional-outbox.md) | Transactional outbox | Ghi trạng thái và event trong cùng một transaction Postgres, một relay đẩy lên Kafka sau |
| [0005](adr/0005-redis-atomic-driver-claim.md) | Giữ chỗ tài xế | Một script Lua trong Redis chiếm tài xế kèm lease, hai đơn không bao giờ cùng nhận một tài xế |
| [0006](adr/0006-retry-tiers-and-dead-letter-topics.md) | Xử lý lỗi | Retry topic với độ trễ tăng dần (5 giây, 1 phút, 10 phút), sau đó vào DLQ; lỗi vĩnh viễn bỏ qua retry; công cụ redrive |
| [0007](adr/0007-avro-and-schema-registry.md) | Hợp đồng event | Avro trong Schema Registry, tương thích `BACKWARD`, field mới phải có giá trị mặc định |
| [0008](adr/0008-notification-fan-out-and-recovery.md) | Thông báo realtime | Đẩy qua WebSocket nhờ Redis pub/sub; đẩy là best-effort, REST là đường khôi phục |
| [0009](adr/0009-location-gateway-and-telemetry-stream.md) | Luồng vị trí | Tài xế không ghi thẳng vào Kafka, gateway giới hạn tốc độ và bỏ báo cáo thừa; độ bền thấp hơn các event giao dịch |
| [0010](adr/0010-payment-ledger-and-provider-idempotency.md) | Thanh toán | Sổ cái kép trong Postgres, nhà cung cấp thẻ nằm sau interface idempotent, tiền là số nguyên |

### Thư mục `specs/ride-booking-mini/` (đặc tả theo kiểu spec-driven)

**[requirements.md](../specs/ride-booking-mini/requirements.md)**: yêu cầu. Mục tiêu dự án, các tác nhân (Rider, Driver), các user story kèm tiêu chí chấp nhận theo dạng EARS ("WHEN … THE system SHALL …"), ràng buộc kỹ thuật và phần ngoài phạm vi.

**[design.md](../specs/ride-booking-mini/design.md)**: thiết kế. Sơ đồ luồng, bảng service (nhận event nào, phát event nào, lưu ở đâu), bảng topic (partition key, số partition), cách chạy surge pricing, Avro và Schema Registry.

**[tasks.md](../specs/ride-booking-mini/tasks.md)**: danh sách việc theo phase 0 đến 7 (nền tảng, Order và Matching, vòng đời chuyến đi, vị trí tài xế, thông báo, độ bền, design system, tích hợp end-to-end) và backlog (đánh giá, tip, surge, loại xe, ksqlDB, Schema Registry, thanh toán). Mọi mục đã đánh dấu xong.

**[production-design.md](../specs/ride-booking-mini/production-design.md)** (khoảng 435 dòng): thiết kế bản production hóa, 16 mục đánh số. Ghi lại từng đợt nâng cấp, các giới hạn đã biết, cách đã kiểm thử, và mục 16 về Payment Service. Đây là nơi nói thẳng hệ thống còn thiếu gì.

### Thư mục `deploy/`

**[deploy/README.md](../deploy/README.md)**: triển khai lên Kubernetes. Cái base manifest triển khai, điều kiện cần có sẵn trong cluster, các secret phải tạo, lệnh áp dụng, cách thay đổi, và script `verify.sh` chứng minh được gì (và không chứng minh được gì).

**[deploy/loadtest/README.md](../deploy/loadtest/README.md)**: cách chạy load test mà không cần Docker, gồm lệnh, vì sao phải chạy `reset.sh` trước mỗi lần đo, và cách đọc kết quả (chỉ so sánh tương đối trên cùng một máy).

### Thư mục `.claude/agents/` (9 file)

Cấu hình các sub-agent của Claude Code, mỗi file khai báo một vai trò. Chúng không phải tài liệu về dự án và bạn không cần đọc khi học.

| File | Vai trò |
|---|---|
| `go-service-dev.md` | Viết và sửa các service Go |
| `frontend-dev.md` | Rider App, Driver App, Ops dashboard, design system |
| `infra-devops.md` | docker-compose, manifest, hạ tầng, tài liệu DR |
| `automation-dev.md` | Script, CI, loadtest |
| `kafka-events-reviewer.md` | Chỉ đọc, rà soát luồng event Kafka/Avro |
| `logging-dev.md` | Log có cấu trúc và tracing |
| `metrics-alerts-dev.md` | Metrics Prometheus, luật cảnh báo |
| `test-writer.md` | Viết và chạy test |
| `spec-planner.md` | Chỉ đọc, lập kế hoạch tính năng theo spec |

## 4. Những điểm dễ nhầm

- **Số đo hiệu năng** (ví dụ 250 đơn/giây) đo trên **một máy 4 nhân** chạy cả hệ thống. Nó cho biết cái gì bị nghẽn, không phải năng lực production.
- **Chưa chạy trên cluster Kubernetes thật.** Các manifest, KEDA, Sentinel, ACL mới được kiểm tra từng phần.
- **Thanh toán là giả lập.** Chỉ có nhà cung cấp sandbox, không có thẻ thật, chi trả tài xế, hoàn tiền hay đối soát.
- **`production-design.md` ghi lại theo thời gian**, nên một số đoạn mô tả trạng thái lúc viết. Khi hai file mâu thuẫn, ưu tiên `README.md` và các ADR vì chúng phản ánh hiện trạng.
