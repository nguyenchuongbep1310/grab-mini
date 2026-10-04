# Hiệu năng: đã đo gì, tìm ra gì

Số liệu đo bằng `deploy/loadtest` (xem README ở đó) trên **một máy 4 core** chạy chung 9 tiến trình Go, Kafka 1 broker, Postgres, Redis và
công cụ sinh tải. Load average lúc chạy 8–13. Vì vậy: **số tuyệt đối chỉ nói máy này làm được gì; giá trị của việc đo là những thứ nó tìm ra.**

## Ba lỗi hiệu năng tìm ra (đã sửa)

| Triệu chứng | Nguyên nhân | Sửa | Trước → sau |
|---|---|---|---|
| Mọi publish đồng bộ mất ≥ 1 s | `kafka.Writer` mặc định `BatchTimeout` = 1 s: gom batch đợi đủ 1 s mới gửi | `kafkaconn.DefaultBatchTimeout` = 2 ms cho mọi writer | 1,001 s → 10,8 ms mỗi publish (acks=all) |
| Consumer vị trí chỉ ~2.300 msg/s, lag tăng | Commit đồng bộ từng message, tuần tự theo reader | `Config.CommitInterval` (commit nền, gom theo thời gian) | ~2.300 → ~5.000 msg/s, lag ≤ 24 |
| Matching ~50 đơn/s | Vòng consume xử lý tuần tự một message mỗi lần | `Config.Workers` / `KAFKA_CONSUMER_WORKERS`: một worker mỗi partition, vẫn đúng thứ tự trong partition | ~50 → vài trăm/s, bị giới hạn bởi máy |
| Matching tốn nhiều lệnh Redis khi tài xế gần đã bận | Mỗi ứng viên một `EVAL` tuần tự (tới 50 round trip/đơn) | `driverstatus.ClaimFirstForOrder`: thử cả danh sách trong **một** script | 1 round trip mỗi đơn |

Hai thay đổi nhỏ kèm theo: `OccurredAt` của event ghi `RFC3339Nano` (metric độ trễ ghép trước đó làm tròn xuống giây), và `Reader.MaxWait`
đặt 500 ms vì `Reader.Close()` chờ fetch đang dở (mặc định 10 s làm shutdown chậm).

## Kết quả cuối (12 partition, 12 worker, 3.000 tài xế báo vị trí mỗi 4 s ≈ 750 báo cáo/s)

| Đơn/s đặt | Ghép được | Độ trễ ghép p99 | Ghi chú |
|---|---|---|---|
| 150 | 100% | 0,1 s | `deploy/loadtest/result-150.json` |
| 250 | 100% | 0,1 s | backlog vị trí ≤ 3 message |
| 400 | ~93%, hàng đợi tăng, timeout | 30–48 s | **máy bão hòa**, không phải Matching |

Ở 400 đơn/s Matching dùng ~0,06 core và handler p95 ~24 ms; Order Service dùng ~0,8 core, handler `order-service-group` p95 160 ms, load
average 11 trên 4 core. Tăng partition/worker từ 12 lên 24 không đổi kết quả (~190 đơn/s ghép được), tức giới hạn là CPU của máy, không phải
cấu trúc consumer. **Chưa có số đo trên hạ tầng thật**; mục tiêu 500 đơn/s và 25.000 vị trí/s trong thiết kế chưa được kiểm chứng.

## Redis GEO: có cần shard không?

`GEOSEARCH ... ASC COUNT 50` quét **mọi tài xế trong bán kính** rồi mới sắp xếp (COUNT không dừng sớm). Chi phí tỉ lệ với số ứng viên trong
bán kính, không phải tổng tài xế online. Đo trên Redis 1 luồng, 1 key:

| Tài xế (trong hộp ~30 km) | Bán kính | Ứng viên | Truy vấn/s (1 core Redis) |
|---|---|---|---|
| 100.000 | 1 km | ~100 | 9.300 |
| 100.000 | 3 km | ~3.300 | 1.350 |
| 1.000.000 | 1 km | ~1.100 | 820 |
| 1.000.000 | 3 km | ~31.000 | **~90** |

`GEOADD`: ~100.000/s; 1 triệu tài xế chiếm ~100 MB.

Kết luận:

1. **Cập nhật vị trí không phải vấn đề** (25.000/s chỉ chiếm ~1/4 một core).
2. **Tìm kiếm mới là chi phí**, và nó phụ thuộc mật độ × bán kính. Chia key theo shard không giảm chi phí *mỗi truy vấn* trong một thành phố
   dày đặc; nó chỉ chia tải cho nhiều node.
3. Đòn bẩy rẻ nhất là **tìm theo vòng mở rộng** (1 km → 3 km → bán kính cấu hình), đã làm trong `matching-service/redis.go`: phần lớn đơn có
   tài xế trống rất gần nên chỉ trả chi phí của vòng nhỏ. Kết quả vẫn là tài xế gần nhất còn trống trong bán kính.
4. **Shard theo ô địa lý đã có, mặc định tắt** (`GEO_SHARD_DEGREES`, ví dụ `0.05` ≈ 5,5 km; xem `pkg/driverlocation`). Mỗi ô là một key
   `drivers:geo:<i>:<j>`; tài xế qua ranh giới thì bị xóa khỏi ô cũ trong cùng script cập nhật; tìm kiếm quét các ô mà hình tròn chạm tới
   (pipeline) rồi gộp theo khoảng cách. Test xác nhận kết quả **giống hệt** có/không shard, kể cả tài xế sát ranh giới và tài xế vừa qua ô.
   **Nó không làm mỗi truy vấn rẻ hơn**: đo 1 triệu tài xế chia ô 0,05°, một truy vấn 3 km vào một ô (27.700 tài xế) vẫn ~9 ms (so với ~11 ms
   ở key đơn), và một truy vấn chạm tới 1–4 ô. Giá trị của nó là **chia tải ra nhiều key**, nên chỉ có ích khi các key đó nằm trên nhiều node
   (Redis Cluster) hoặc nhiều Redis; trên một Redis duy nhất thì không đáng bật. Ràng buộc:
   - Matching và Driver Location Service phải cùng giá trị (ConfigMap dùng chung `GEO_SHARD_DEGREES`); đổi giá trị cần dựng lại GEO set
     (tài xế tự điền lại sau vài giây báo vị trí), vì vị trí đã lưu nằm ở bố cục cũ.
   - Bán kính phải không quá ~64 ô; vượt thì `Search` trả lỗi thay vì lặng lẽ bỏ sót tài xế (tăng cỡ ô hoặc giảm bán kính).
   - Trên Redis Cluster, các key ô và script dựng key động cần hash tag; **chưa làm**, nên chưa dùng được với Cluster.
   - `ClaimFirstForOrder` cũng dựng key `driver:<id>:*` trong script, cùng giới hạn Cluster.
