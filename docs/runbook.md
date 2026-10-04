# Runbook

Cái gì hỏng, kiểm tra ở đâu, làm gì. Mỗi mục dưới đây ứng với một cảnh báo trong
[deploy/monitoring/alerts.rules.yaml](../deploy/monitoring/alerts.rules.yaml): trường `runbook`
của cảnh báo trỏ tới tiêu đề của mục (đừng đổi tiêu đề mà không đổi cảnh báo).

> **Phạm vi kiểm chứng.** Các công cụ của project (`dlq-redrive`, truy vấn Postgres, khóa Redis,
> metric, log) đã được thử trên môi trường test cục bộ. Các lệnh `kubectl` và tài nguyên Strimzi/KEDA
> **chưa được chạy trên một cluster thật**: manifest đã qua kiểm tra schema và kiểm tra chéo
> (`deploy/verify.sh`) chứ chưa được áp dụng. Lần đầu dùng, hãy thử ở môi trường staging.

## Cách tìm hiểu một sự cố

1. **Cảnh báo nói gì?** Tên cảnh báo cho biết hệ thống nào. `severity: page` nghĩa là rider bị ảnh hưởng
   hoặc dữ liệu đang gặp rủi ro ngay bây giờ.
2. **Pod nào không khỏe?** `/readyz` của mỗi service trả JSON nói dependency nào lỗi
   (cổng admin `9090`, không phải cổng API):
   ```bash
   kubectl -n grab-mini port-forward deploy/trip-service 9090 &
   curl -s localhost:9090/readyz    # {"checks":{"kafka":"ok","postgres":"ok","redis":"connection refused"},"status":"unavailable"}
   ```
3. **Một đơn cụ thể đi đâu?** Log là JSON, mỗi dòng có `service`; dòng nằm trong một span có thêm
   `trace_id`. Tìm theo `orderId`, hoặc theo `trace_id` để xem mọi service đã làm gì cho cùng một request
   (HTTP, outbox, từng hop Kafka, cả các lần retry, đều ở trong một trace).
4. **Trạng thái consumer:**
   ```bash
   kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP --command-config ops.properties --describe --group matching-service-group
   ```
   (`ops.properties` là credential của người vận hành, xem `deploy/kafka-secure` và Strimzi `KafkaUser/ops`.)

Các dashboard nên có: lag theo group, `kafka_consumer_messages_total` theo `result`,
`outbox_oldest_pending_age_seconds`, `matching_latency_seconds`, `http_requests_total` theo route và code.

---

## Consumer lag

**Cảnh báo:** `KafkaConsumerLagGrowing`, `KafkaLocationConsumerLagGrowing`.
**Nghĩa là:** một consumer group xử lý chậm hơn tốc độ message đến. Rider thấy trạng thái chậm hoặc thiếu.

Group `*.retry.N` có lag là bình thường (message chờ đến hạn), cảnh báo đã loại chúng ra.

**Kiểm tra theo thứ tự:**

1. Lag có thật sự tăng, hay vừa có một đợt cao điểm? Xem `kafka_consumergroup_lag` theo `partition`:
   - **Một partition lag, các partition khác không:** một message "nặng" hoặc một key nóng. Tìm trong log
     `topic`, `partition`, `offset` của message đang xử lý.
   - **Mọi partition đều lag:** consumer thiếu năng lực hoặc dependency chậm.
2. Handler có chậm không? `histogram_quantile(0.95, sum by (le, group) (rate(kafka_consumer_handle_duration_seconds_bucket[5m])))`.
   Tăng đột ngột nghĩa là Postgres hoặc Redis chậm (xem `/readyz`, và mục [Pods not ready](#pods-not-ready)).
3. Có đang retry hàng loạt không? `rate(kafka_consumer_messages_total{result="retried"}[5m])`. Nếu có, nguyên nhân
   là một dependency lỗi: xem [Dead letters](#dead-letters).
4. KEDA có đang scale không? `kubectl -n grab-mini get scaledobject` và `describe` cái liên quan.
   Số replica **bị chặn bởi số partition** (6 hoặc 12 hoặc 48 tùy topic): thêm replica vượt số partition
   không giúp gì. Nếu replica đã ở mức tối đa mà vẫn lag, cần tăng số partition (chỉ tăng được, và làm đổi
   ánh xạ key → partition, nên thứ tự trong lúc chuyển có thể lệch tạm thời) hoặc làm handler nhanh hơn.
5. Rebalance liên tục? Log có `consumer stopped` / `consumer running` lặp lại, hoặc pod restart: xem
   `kubectl get pods` (RESTARTS), và `kubectl describe pod` (OOMKilled?).

**Không nên làm:** reset offset để "bỏ qua" lag. Message bị bỏ qua là event không bao giờ được xử lý
(đơn không được ghép, rating không được đếm).

## Dead letters

**Cảnh báo:** `KafkaMessagesDeadLettered` (page, bất kỳ message nào), `KafkaConsumerFailureRatioHigh`.
**Nghĩa là:** một message không xử lý được sau mọi lần thử lại, nên đã được cất vào `<topic>.DLQ`. Event đó
**chưa được xử lý**.

1. **Xem nó là gì.** Mỗi dead letter là JSON: `originalTopic`, `error`, `retries`, `valueText` (nếu là text)
   và `valueBase64` (bytes gốc). Đọc bằng người vận hành:
   ```bash
   kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP --consumer.config ops.properties \
     --topic order.created.DLQ --from-beginning --max-messages 5
   ```
   Hoặc xem trước mà không đổi gì: `dlq-redrive -topic order.created -dry-run`.
2. **Phân loại theo `error` và `retries`:**
   | Dấu hiệu | Nguyên nhân thường gặp | Việc cần làm |
   |---|---|---|
   | `retries: 0`, lỗi `not a Schema Registry Avro message` hoặc `isn't readable with the ... schema` | Ai đó ghi message sai định dạng, hoặc đổi schema không tương thích | Sửa nguồn ghi. Message hỏng thật thì **không redrive**: nó sẽ rơi lại vào DLQ |
   | `retries: 3` (đủ các tầng), lỗi kết nối `postgres`/`redis` | Dependency đã lỗi một lúc | Chờ dependency khỏe, rồi redrive |
   | Lỗi nghiệp vụ lặp lại với cùng một message | Một bug trong handler | Sửa code, deploy, rồi redrive |
3. **Redrive** sau khi nguyên nhân đã được sửa. Redrive đẩy message về topic gốc với **đúng key và bytes**; consumer
   xử lý lại (consumer idempotent nên xử lý hai lần là an toàn):
   ```bash
   # xem sẽ làm gì
   dlq-redrive -topic order.created -dry-run
   # chỉ một nguyên nhân, tối đa 100 message một lần
   dlq-redrive -topic order.created -error "connection refused" -max 100
   # tất cả
   dlq-redrive -topic order.created
   ```
   Cần credential của người vận hành (các principal của service **không** đọc được DLQ, có chủ ý), cấu hình bằng
   các biến `KAFKA_*` như mọi service (`KAFKA_BROKERS`, `KAFKA_SECURITY_PROTOCOL`, `KAFKA_USERNAME`,
   `KAFKA_PASSWORD_FILE`, `KAFKA_CA_FILE`). Trên Kubernetes, chạy như một Job dùng image `dlq-redrive` với secret của user `ops`.
   - Chạy lại lần nữa chỉ gửi những gì mới: vị trí được nhớ trong consumer group `dlq-redrive-<topic>`
     (mỗi bộ lọc `-error` có group riêng, vì một group không thể bỏ qua message).
   - Message nào lại thất bại sẽ rơi vào DLQ lần nữa như một dead letter mới.
4. **Kiểm tra sau đó:** `kafka_consumer_messages_total{result="dead_lettered"}` không tăng nữa, lag về 0.

**Riêng `driver.location.updated`:** dead letter ở đây không có cảnh báo page (vị trí cũ vô giá trị).
Đừng redrive: vị trí mới sẽ đến sau vài giây, và redrive còn bị loại vì `timestamp` cũ.

## Kafka broker down

**Cảnh báo:** `KafkaBrokersMissing`, `KafkaUnderReplicatedPartitions`, `KafkaPublishErrors`, `KafkaPublishSlow`.

Cấu hình: 3 broker, mỗi zone một cái, `replication.factor=3`, `min.insync.replicas=2`
(topic vị trí: `1`), không bầu leader không đồng bộ.

- **Mất 1 broker** (hoặc 1 zone): hệ thống **vẫn ghi được** (còn 2 replica đồng bộ), nhưng không còn dư. Mất thêm một
  nữa là producer bị `NotEnoughReplicas`. Coi đây là sự cố cần xử lý ngay dù người dùng chưa thấy gì.
- **Mất 2 broker:** các topic nghiệp vụ không ghi được.
  - Order Service và Trip Service: event nằm trong bảng `outbox` (đã commit cùng thay đổi trạng thái), **không mất**.
    `outbox_oldest_pending_age_seconds` tăng, xem [Outbox backlog](#outbox-backlog). Khi broker quay lại, relay tự gửi bù.
  - Matching Service và Location Gateway publish trực tiếp, **không có outbox**. Gateway trả 503 và tài xế gửi lại
    vị trí sau vài giây. Matching: nếu không publish được `order.matched`, handler trả lỗi và trả tài xế về (`ReleaseMatch`),
    message đi theo đường retry. Nếu cả retry topic lẫn DLQ cũng không ghi được, consumer **giữ partition lại**
    (log `cannot put a failed message on a retry or dead-letter topic`, lag tăng) thay vì commit và làm mất message,
    rồi tiếp tục khi Kafka ghi được trở lại.

**Kiểm tra:**
```bash
kubectl -n grab-mini get kafka,kafkanodepool,pods -l strimzi.io/cluster=grab-mini
kubectl -n grab-mini describe kafka grab-mini           # điều kiện Ready, lý do
kubectl -n grab-mini logs grab-mini-broker-1            # ví dụ pod broker
```
`kafka_topic_partition_under_replicated_partition` cho biết topic và partition nào; `kafka_brokers` cho biết số broker.

**Xử lý:** broker thường tự quay lại (pod restart, chuyển node). Nếu không:
1. Pod ở trạng thái `Pending`? Thường là thiếu tài nguyên hoặc PVC không gắn được ở zone đó
   (`kubectl describe pod`): broker phải chạy ở đúng zone có đĩa của nó.
2. Đĩa đầy? `kubectl exec ... -- df -h /var/lib/kafka`. Giảm `retention.ms` của topic lớn nhất (thường
   `driver.location.updated`) hoặc nở PVC.
3. **Đừng** bật `unclean.leader.election.enable` để "cho chạy lại": nó đổi dữ liệu đã được xác nhận lấy tính sẵn sàng.

**Sau khi khỏe lại:** URP về 0, `outbox_oldest_pending_age_seconds` về 0, lag giảm. Không cần redrive: không có
message nào bị dead-letter chỉ vì broker tạm mất (chúng được retry).

## Outbox backlog

**Cảnh báo:** `OutboxBacklog` (event chờ quá 30 giây), `OutboxEventQuarantined`.
**Nghĩa là:** trạng thái đã được lưu nhưng **chưa được thông báo**: rider và các service khác chưa biết.

1. **Xem bảng** (Postgres của Order hoặc Trip):
   ```sql
   SELECT source, count(*) AS waiting, min(created_at) AS oldest FROM outbox GROUP BY source;
   SELECT id, source, topic, failed_at, left(error, 120) FROM outbox_dead ORDER BY failed_at DESC LIMIT 20;
   ```
2. **Relay có chạy không?** Log của order-service/trip-service tìm `[outbox:...] publish failed`: lý do nằm ngay đó.
   - `connection refused` / `broker not available`: xem [Kafka broker down](#kafka-broker-down).
   - `Topic Authorization Failed`: ACL của service bị đổi hoặc thiếu (`KafkaUser` của chính nó cần `Write` trên topic).
     Sửa ACL; relay sẽ tự gửi bù, không cần làm gì thêm.
   - `Schema Registry`: không liên quan relay (payload đã được mã hóa Avro lúc request); lỗi này gặp ở lúc tạo đơn.
3. **Relay bị kẹt bởi khóa?** Mỗi source chỉ một relay chạy tại một thời điểm (advisory lock). Nếu một pod treo giữ
   transaction mở, các pod khác bỏ qua:
   ```sql
   SELECT pid, state, now() - xact_start AS open_for, left(query, 80) FROM pg_stat_activity WHERE state <> 'idle' ORDER BY xact_start;
   ```
   Giao dịch mở rất lâu của relay: khởi động lại pod đó.
4. **`outbox_dead`** (cảnh báo `OutboxEventQuarantined`): relay đã **bỏ ra ngoài** một event mà Kafka từ chối vĩnh viễn
   (thường là quá lớn so với `message.max.bytes`, hoặc topic không hợp lệ), để các event sau không bị chặn.
   Event đó **sẽ không được gửi**: trạng thái đã lưu nhưng không ai được báo.
   - Đọc `error`, `topic` và kích thước (`length(payload)`). Payload là Avro nhị phân nên không đọc được dưới dạng text.
   - Sửa nguyên nhân (thường là một bug sinh payload quá lớn).
   - Muốn gửi lại sau khi sửa: chép dòng về `outbox` rồi xóa khỏi `outbox_dead`:
     ```sql
     BEGIN;
     INSERT INTO outbox (source, topic, key, payload, headers, created_at)
       SELECT source, topic, key, payload, headers, now() FROM outbox_dead WHERE id = <id>;
     DELETE FROM outbox_dead WHERE id = <id>;
     COMMIT;
     ```
     Chỉ làm vậy khi Kafka sẽ chấp nhận nó lúc này. Nếu không, hãy quyết định nghiệp vụ: bù trạng thái bằng tay.

## Slow matching

**Cảnh báo:** `MatchLatencySLOBreach` (p95 trên 3 giây), `NoDriverRatioHigh` (trên 30% đơn không có tài xế).

Thời gian ghép = từ lúc `order.created` được tạo đến lúc `order.matched` được publish
(`matching_latency_seconds`). Khi chưa có tài xế, Matching thử lại `MATCH_RETRIES` (mặc định 3) lần, cách nhau
`MATCH_RETRY_DELAY_MS` (mặc định 2000 ms): một đơn **không tìm được** tài xế mất khoảng 6 giây trước khi báo
`order.no_driver_found`, và con số đó tính vào độ trễ. Vì vậy hai cảnh báo thường đi cùng nhau.

1. **Lag của `matching-service-group`** trên `order.created`: xem [Consumer lag](#consumer-lag). Đơn nằm chờ trong topic.
2. **Redis chậm hoặc đứt:** `/readyz` của matching-service (cổng admin). Matching phụ thuộc Redis cho `drivers:geo`
   và trạng thái tài xế.
3. **Thiếu tài xế thật, hay dữ liệu sai?** `rate(location_updates_total{result="applied"}[5m])` có đều không?
   Nếu vị trí không được cập nhật, Driver Location Service hoặc Location Gateway hỏng
   (`location_reports_total` theo `result`: `published`, `stationary`, `too_fast`, `invalid`, `error`).
4. **Tài xế kẹt trạng thái.** Matching chỉ nhận tài xế `AVAILABLE` (hoặc `ASSIGNED` đã hết lease, mặc định 120 giây).
   Tài xế `ON_TRIP` mà không còn trip nào đang chạy là kẹt. Kiểm tra bằng cả hai nguồn:
   ```sql
   -- Postgres của Trip Service: tài xế có trip đang chạy không?
   SELECT driver_id, id, status FROM trips WHERE driver_id = 'driver-7' AND status IN ('ASSIGNED','IN_PROGRESS');
   ```
   ```bash
   redis-cli GET driver:driver-7:status      # AVAILABLE | ASSIGNED | ON_TRIP
   redis-cli GET driver:driver-7:assigned_until
   ```
   Nếu **không có trip đang chạy** nhưng Redis vẫn `ON_TRIP`, consumer `trip-driver-release-group` đã bị trễ hoặc hỏng
   (lag của nó); sau khi sửa, nó tự giải phóng. Giải phóng bằng tay chỉ khi chắc chắn không còn trip:
   `redis-cli SET driver:driver-7:status AVAILABLE` rồi `redis-cli DEL driver:driver-7:assigned_until`.
5. **Vị trí cũ:** `drivers:geo` giữ vị trí mới nhất mỗi tài xế; tài xế ngừng gửi vị trí vẫn nằm trong GEO set với vị trí
   cũ. Không có TTL: một tài xế offline vẫn có thể bị ghép. (Đây là giới hạn đã biết, xem production-design.)

## Booking errors

**Cảnh báo:** `OrderCreationErrorBudgetBurn` (page), `HTTPServerErrorsHigh`.
Mục tiêu: 99,9% `POST /orders` thành công. Cảnh báo kêu khi ngân sách lỗi đang cháy nhanh gấp 14 lần, trên cả cửa sổ 5 phút lẫn 1 giờ.

`POST /orders` làm: kiểm tra token, mã hóa Avro (cần **Schema Registry**), ghi Postgres (đơn + outbox trong một transaction).
Mã 5xx nghĩa là một trong ba thứ đó hỏng. Phân biệt bằng log của order-service (`failed to encode order.created`
→ Schema Registry; `failed to save order` → Postgres) hoặc `/readyz` (Postgres, Kafka).

- **Schema Registry down:** mỗi service nhớ id schema của mình sau lần đăng ký đầu, nên pod **đang chạy** vẫn nhận đơn
  bình thường. Lỗi 500 xuất hiện ở pod **mới khởi động** (chưa đăng ký được) và ở consumer gặp `schema id` chưa từng
  thấy (một phiên bản schema mới). Vì vậy sự cố registry thường lộ ra sau một lần deploy hoặc scale-out, không phải ngay
  lúc registry chết. Khôi phục registry, rồi kiểm tra `/readyz`.
- **Postgres down:** xem [Pods not ready](#pods-not-ready); failover của Postgres là việc của nhà cung cấp/DBA.
- **401 tăng đột ngột** (không phải 5xx, nhưng người dùng thấy lỗi): `JWT_SECRET` không đồng nhất giữa các service
  hoặc token hết hạn hàng loạt. Tất cả service verify và nhà phát hành token phải dùng cùng một secret.

## Pods not ready

**Cảnh báo:** `DeploymentReplicasUnavailable`.

1. `kubectl -n grab-mini get pods`, rồi `kubectl -n grab-mini describe pod <pod>` (events: `Unhealthy`, `OOMKilled`, `ImagePullBackOff`).
2. Pod `Running` nhưng `0/1 Ready`: `/readyz` (cổng admin) nói dependency nào lỗi, xem ở đầu tài liệu.
   Service không sẵn sàng khi **dependency** của nó lỗi, và đó là hành vi đúng: nó rời khỏi vòng phục vụ mà không bị restart.
   Một pod không bao giờ bị restart vì Postgres down (liveness `/livez` không phụ thuộc gì bên ngoài).
3. Rollout kẹt sau khi deploy: `kubectl rollout status deploy/<name>`; pod mới không `Ready` thì pod cũ được giữ
   (`maxUnavailable: 0`), nên người dùng không bị ảnh hưởng. `kubectl rollout undo deploy/<name>` để quay lại, nếu
   migration đã chạy là tương thích ngược (xem bên dưới).

---

## Quy trình thường gặp

### Deploy

1. Build và đẩy image; đặt tag: `kustomize edit set image ghcr.io/example/grab-mini/order-service=<registry>/order-service:<tag>` (mỗi service).
2. **Migration trước**: chạy các Job `*-migrate` và chờ xong (`kubectl wait --for=condition=complete job/order-service-migrate`).
   Migration chỉ tiến, và phải tương thích với **cả phiên bản cũ lẫn mới** của code (thêm cột có default, không xóa/đổi tên cột
   khi bản cũ còn chạy), vì rolling update cho hai bản chạy song song.
3. `kubectl apply -k deploy/k8s/base` (hoặc overlay của môi trường). Theo dõi `kubectl rollout status`.
4. Đổi **schema Avro**: chỉ thêm trường có default (registry chạy `BACKWARD`). Đăng ký schema trong CI trước khi deploy.

### Replay một topic (xử lý lại từ đầu)

Chỉ làm được vì mọi consumer idempotent (README: "Replay a topic and see nothing counted twice"). Dừng KEDA trước,
nếu không nó sẽ scale lại lên:
```bash
kubectl -n grab-mini annotate scaledobject/rating-service autoscaling.keda.sh/paused-replicas=0 --overwrite
kubectl -n grab-mini scale deploy/rating-service --replicas=0
kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP --command-config ops.properties \
  --group rating-service-group --reset-offsets --to-earliest --topic trip.rated --execute
kubectl -n grab-mini annotate scaledobject/rating-service autoscaling.keda.sh/paused-replicas-   # bỏ tạm dừng
```
Không replay `driver.location.updated` (vị trí cũ bị loại vì `timestamp`) hay topic có side effect không idempotent.

### Xoay vòng secret

- **Mật khẩu Kafka của một service:** xóa Secret do Strimzi tạo cho `KafkaUser` đó để nó sinh lại, rồi restart pod của service
  (mật khẩu chỉ được đọc lúc khởi động). Làm lần lượt từng service.
- **`JWT_SECRET`:** dùng chung HS256, nên đổi là **mọi token hiện có mất hiệu lực**. Đổi Secret `grab-mini-jwt` rồi restart
  *tất cả* service cùng lúc (nhà phát hành token và các service verify). App tự lấy token mới khi nhận 401.
- **Mật khẩu Postgres:** đổi trong Secret `grab-mini-db-<service>` rồi restart; service đọc `DATABASE_URL` lúc khởi động.

### Scale tay

KEDA điều khiển số replica; `kubectl scale` bị KEDA ghi đè. Đổi `minReplicaCount`/`maxReplicaCount` trong `ScaledObject`
(qua `deploy/k8s/generate.py`), hoặc tạm dừng bằng annotation `autoscaling.keda.sh/paused-replicas` như trên.
Số replica hữu ích của một consumer không vượt số partition của topic nó đọc.

### Điều chưa có (cần biết khi trực)

- **Không có outbox cho Matching và Location Gateway**: nếu Kafka hỏng đúng lúc, một lần ghép có thể không được thông báo
  (đã có lease để tài xế không kẹt, và message `order.created` đi qua retry), còn vị trí tài xế thì tự bù sau vài giây.
- **Không có TTL cho vị trí trong Redis GEO** (xem Slow matching).
- **Notification Service best-effort**: tin gửi khi không ai subscribe thì mất; app khôi phục bằng REST sau khi kết nối lại.
- **Sao lưu và DR đa vùng chưa làm** (giai đoạn 5).
