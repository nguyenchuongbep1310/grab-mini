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
4. **Shard theo ô địa lý** (key `drivers:geo:<ô>`, truy vấn ô chứa điểm đón và các ô kề) chỉ đáng làm khi một Redis không còn chịu nổi: khoảng
   vài trăm đơn/s cùng lúc trong một khu rất đông, hoặc cần Redis Cluster. **Chưa làm, vì chưa có số đo thật cho thấy cần.** Lưu ý nếu làm:
   tài xế ở rìa ô phải xuất hiện ở cả ô kề hoặc truy vấn phải quét ô kề; di chuyển qua ranh giới ô phải xóa khỏi ô cũ; script
   `ClaimFirstForOrder` tự dựng key `driver:<id>:*` nên trên Redis Cluster các key đó phải dùng hash tag.
