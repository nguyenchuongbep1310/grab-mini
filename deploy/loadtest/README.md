# Load test

Đo cả đường đi thật của một chuyến (đặt xe → ghép tài xế → trip → đón → hoàn tất → đánh giá) cùng luồng vị trí tài xế, **không cần Docker**: chạy
các service Go thật trên một broker Kafka, Postgres và Redis có sẵn.

```
export KAFKA_BIN=/path/to/kafka/bin KAFKA_BROKERS=localhost:9092 REDIS_ADDR=localhost:6380 \
       PSQL_ADMIN='psql -h /tmp -p 5433 -U postgres' DB=grabmini_load \
       DATABASE_URL='postgres://postgres@/grabmini_load?host=/tmp&port=5433' \
       PARTITIONS=12 KAFKA_CONSUMER_WORKERS=12

deploy/loadtest/reset.sh        # XÓA topic của app, tạo lại DB, FLUSH Redis. Chỉ trỏ vào hạ tầng bỏ đi.
deploy/loadtest/stack.sh start  # build + chạy dev-registry (Schema Registry giả) và 8 service
go build -o /tmp/loadtest ./services/loadtest   # (workspace: cd services/loadtest && go build)
/tmp/loadtest -redis $REDIS_ADDR -drivers 3000 -orders-per-sec 250 -duration 40s -drain 8s -poll-interval 1s
deploy/loadtest/stack.sh stop
```

Chạy `reset.sh` **trước mỗi lần đo**: backlog còn lại từ lần trước làm sai số liệu, và service phải khởi động sau khi topic đã có (consumer
khởi động trước topic sẽ không bao giờ được gán topic đó).

Công cụ sinh tải mở (open loop: đặt đơn theo nhịp cố định dù server chậm, nên hàng đợi lộ ra thay vì bị che) và in hai nhóm số liệu:
phía client (độ trễ từng thao tác qua gateway) và phía server (quantile tính từ delta của `/metrics`: độ trễ ghép, lag, CPU từng tiến trình).
`-out file.json` lưu toàn bộ kết quả.

## Đọc kết quả

- `create_to_matched` p50 ≈ `-poll-interval` là bình thường: client hỏi mỗi giây một lần. Số đúng là `match latency` phía server.
- Có `timeout:` ở `create_to_matched` hoặc `NOT cleared within the drain` ở backlog nghĩa là hệ thống **không theo kịp** tải đó.
- Chỉ so sánh **tương đối** giữa các lần chạy trên cùng một máy. Cả stack, broker, Postgres, Redis và công cụ sinh tải dùng chung vài core,
  nên số tuyệt đối không phải năng lực của production. Kết quả và cách diễn giải: [docs/performance.md](../../docs/performance.md).
