# Ingestion: từ Wikimedia đến Kafka và HDFS

Code: [`src/producer/sse_kafka_producer.py`](../src/producer/sse_kafka_producer.py), [`src/streaming/raw_to_hdfs.py`](../src/streaming/raw_to_hdfs.py).

## Producer làm gì

1. Mở kết nối SSE tới `stream.wikimedia.org/v2/stream/recentchange` với User-Agent mô tả rõ ứng dụng (Wikimedia yêu cầu).
2. Với mỗi event: ghi `producer_received_at`, parse JSON, bỏ qua canary, bọc thành message theo [data contract](03_data_contract.md).
3. Gửi vào Kafka với idempotence bật (`acks=all`, retry an toàn).
4. Mỗi 30 giây ghi một dòng log `stats`: số đếm tích lũy, hàng đợi và p50/p95 source-to-producer của 30 giây vừa qua.

## Xử lý lỗi

| Tình huống | Producer làm gì | Log để kiểm chứng |
|---|---|---|
| Server đóng stream | Kết nối lại, gửi `Last-Event-ID` | `SSE disconnected`, `SSE gap closed gap_s=...` |
| Mất mạng, read timeout 60 giây | Như trên, backoff 1 giây tăng dần tới 60 giây, có jitter | Như trên |
| HTTP 429 | Chờ theo `Retry-After`, không có thì dùng backoff | `SSE disconnected ... HTTP 429` |
| Event không phải JSON object | Bỏ qua, đếm `malformed` | `malformed event` |
| Event SSE loại `error` | Không gửi Kafka, đếm `sse_error_events` | `SSE error event` |
| Kafka tạm thời không truy cập được | Giữ trong hàng đợi và retry không giới hạn thời gian (`message.timeout.ms=0`) | `kafka client error`, `queued` tăng |
| Hàng đợi đầy (20.000 message) | `produce()` chờ, nên ngừng đọc SSE. Khi Kafka sống lại thì tiếp tục đọc, hoặc resume bằng `Last-Event-ID` nếu server đã ngắt | `queue_full_waits` |
| Kafka từ chối vĩnh viễn một message | Ghi lỗi, đếm `delivery_failed` | `delivery failed` |
| Lỗi Kafka fatal | Thoát để container restart | `fatal kafka error` |
| SIGTERM, SIGINT | Flush tối đa 30 giây, log số còn lại | `shutdown final_counts=... undelivered=...` |

### Giới hạn đã biết

- `Last-Event-ID` chỉ nằm trong bộ nhớ. Khi producer restart, các event phát ra trong lúc nó tắt không được lấy lại. Khoảng trống đó thấy được qua `missing` trong báo cáo chất lượng.
- Resume có thể gửi lại event, nên chặng SSE đến Kafka là at-least-once, không phải exactly-once. Idempotence của Kafka chỉ chống trùng do retry trong một phiên producer, không chống trùng do replay từ SSE.
- `gap_s` tính từ lúc producer phát hiện mất kết nối, không phải từ lúc mạng đứt. Khi mạng đứt im lặng, việc phát hiện có thể trễ tới 60 giây (read timeout), nên `gap_s` thấp hơn thời gian đứt thật. Bằng chứng mất dữ liệu đáng tin hơn là `source_offsets[].missing`.
- Event replay sau khi đứt kết nối có `source_to_producer` lớn, vì `producer_received_at` là lúc nhận lại. p95 toàn phiên có đứt kết nối phản ánh sự cố, không phải độ trễ lúc ổn định. Muốn số lúc ổn định thì xem dòng `stats` của từng khoảng 30 giây.

## Raw sink

`raw_to_hdfs.py` là một streaming query riêng, có checkpoint riêng tại `/checkpoints/raw_to_hdfs`. Mỗi 30 giây nó ghi Parquet vào `year=YYYY/month=MM/day=DD/hour=HH/`.

Về đảm bảo của chặng này, file sink của Spark ghi danh sách file đã commit vào `_spark_metadata`. Vì vậy phải đọc bằng Spark theo thư mục (`spark.read.parquet(path)`). Đọc trực tiếp từng file có thể thấy file dở dang của batch bị crash.

Khi Kafka không truy cập được khoảng một phút, query có thể dừng với lỗi `Timed out waiting for a node assignment`. Khởi động lại query với checkpoint cũ thì nó đọc tiếp từ offset đã dừng.

Spark core, checkpoint cho analytics và bài test crash recovery chính thức thuộc phần của Giang. Tài liệu này chỉ nói về raw sink.

## Báo cáo chất lượng

[`src/streaming/quality_report.py`](../src/streaming/quality_report.py) đọc raw Parquet và xuất JSON vào `outputs/experiment_results/raw_quality_report.json`:

| Trường | Ý nghĩa |
|---|---|
| `total_rows`, `event_time_range` | Quy mô và khoảng thời gian của dữ liệu |
| `missing_by_field` | Số dòng thiếu từng trường |
| `duplicate_event_id_rows` | Số dòng trùng `event_id` |
| `source_offsets[].missing` | Số offset nguồn bị hở: event Wikimedia đã phát nhưng không vào raw. Event canary bị producer bỏ cũng tạo lỗ, nên so với `canary_skipped` trong log trước khi kết luận mất dữ liệu. |
| `rows_by_type`, `rows_by_bot_value` | Phân bố giá trị thô |
| `parquet_schema` | Schema thực tế của file Parquet |
| `latency` | p50, p95, min, max, số mẫu, số mẫu âm của cả ba loại latency |
