# Kiến trúc hệ thống

Tài liệu này mô tả các thành phần, luồng dữ liệu, cách chia query và cách chọn cấu hình. Phiên bản từng thành phần nằm ở [04_technology_stack.md](04_technology_stack.md), hợp đồng dữ liệu ở [03_data_contract.md](03_data_contract.md).

## 1. Sơ đồ

```text
Wikimedia EventStreams (SSE)
          |
          v
Python SSE Producer                 (src/producer/sse_kafka_producer.py)
 - bỏ event canary
 - thêm event_id, event_time, producer_received_at
 - key Kafka = wiki:title
          |
          v
Kafka topic: wikipedia-recent-changes   3 partition, retention 24 giờ
          |
          +-------------------------------+
          |                               |
          v                               v
Query raw_to_hdfs                   Query analytics (3 query con)
 append, trigger 30 giây             event-time window + watermark
 checkpoint riêng                    checkpoint riêng cho từng query
          |                               |
          v                               v
HDFS Parquet                        Kafka topic: wikipedia-analytics
/data/raw/recent_changes/            edit_count, bot_ratio, activity_spike
year=/month=/day=/hour=                     |
          |                                 v
          v                           Streamlit Dashboard
quality_report.py (batch)
 chất lượng dữ liệu + 3 loại latency
```

Mọi dịch vụ chạy trong `docker-compose.yml` trên một network `wiki-net`.

## 2. Vì sao chia như vậy

| Quyết định | Lý do | Đánh đổi |
|---|---|---|
| Kafka nằm giữa producer và Spark | Spark chết thì producer vẫn gửi; Spark bật lại đọc tiếp từ offset trong checkpoint. Đây chính là điều kiện để bài test recovery có ý nghĩa. | Thêm một dịch vụ và một nguồn tiêu thụ RAM |
| Raw và Analytics là hai nhánh query riêng, đọc cùng một topic | Dashboard không phải quét lại raw; lỗi ở một nhánh không làm nhánh kia dừng | Dữ liệu được đọc từ Kafka hai lần |
| Mỗi streaming query một `checkpointLocation` riêng | Checkpoint lưu offset và state của riêng query đó. Dùng chung sẽ làm hai query ghi đè tiến độ của nhau. | Nhiều thư mục checkpoint phải quản lý |
| Checkpoint đặt trên HDFS (`hdfs://namenode:8020/checkpoints/...`) | Bền vững qua việc xóa và tạo lại container. Checkpoint trong filesystem container sẽ mất, và bài test recovery sẽ vô nghĩa. | Phụ thuộc HDFS phải sống trước khi submit job |
| Analytics output đi qua Kafka topic thay vì database | Không thêm thành phần mới; Spark và Streamlit tích hợp độc lập qua một hợp đồng JSON | Topic không phục vụ truy vấn lịch sử; Dashboard phải tự giữ state hiển thị |
| `outputMode("append")` cho cả ba metric | Có watermark thì mỗi window chỉ phát một lần khi đóng, nên Dashboard không phải lọc bản ghi lặp của cùng một window | Chỉ thấy kết quả sau khi window đóng, nên trễ hơn `update` |
| Spark chạy `local[2]` trong một container | Máy test chỉ còn khoảng 3,7 GB cho container, thêm master và worker daemon sẽ lấy RAM của Kafka và HDFS | Không minh họa được phân phối task nhiều executor |
| `failOnDataLoss=false` ở Kafka source | Retention 24 giờ có thể xóa offset cũ trong lúc query đang chết. Query phải chạy tiếp thay vì tắt. | Nếu mất dữ liệu thật thì chỉ có cảnh báo trong log |
| RocksDB state store | State của sliding window 2 phút với watermark 5 phút nằm ngoài heap JVM | Thêm I/O đĩa cục bộ |

## 3. Các query

| Query | Nguồn | Phép toán | Sink | Checkpoint |
|---|---|---|---|---|
| `raw_to_hdfs` | topic raw | parse envelope, không tổng hợp | Parquet trên HDFS | `/checkpoints/raw_to_hdfs` |
| `analytics_edit_count` | topic raw | tumbling 1 phút, theo `wiki` | topic analytics | `/checkpoints/analytics_edit_count` |
| `analytics_bot_ratio` | topic raw | tumbling 5 phút, Bot/Human/Unknown | topic analytics | `/checkpoints/analytics_bot_ratio` |
| `analytics_activity_spike` | topic raw | sliding 2 phút trượt 30 giây, theo `wiki`, `title` | topic analytics | `/checkpoints/analytics_activity_spike` |

Ba query analytics chạy trong cùng một ứng dụng Spark (`src/streaming/analytics_job.py`) nhưng là ba streaming query độc lập: mỗi query có tiến độ, state và checkpoint riêng.

## 4. Event time và watermark

- Window và watermark dùng `event_time` (từ `meta.dt`), không dùng thời điểm Spark xử lý. Nhờ vậy một event đến trễ vẫn được tính vào window đúng của nó, miễn là còn trong giới hạn watermark.
- Watermark là mốc "không chờ thêm nữa": Spark coi mọi event có event time cũ hơn `max(event_time) - watermark` là quá trễ, đóng window và bỏ state tương ứng. Watermark **không** phải bộ lọc bỏ mọi event cũ hơn một khoảng cố định.
- Watermark lớn giữ state lâu hơn, nên nhận được nhiều event trễ hơn nhưng tốn bộ nhớ hơn và kết quả ra chậm hơn. Số liệu so sánh 10 giây / 1 phút / 5 phút nằm ở [05_experiments_and_evaluation.md](05_experiments_and_evaluation.md).
- Giá trị mặc định đang dùng: `WATERMARK=1 minute`. Trên 20.000 event replay, cấu hình này chỉ bỏ 5 dòng (so với 118 dòng ở 10 giây) mà state chỉ bằng một nửa cấu hình 5 phút, và window đóng sớm hơn 4 phút. Số liệu đầy đủ ở [05_experiments_and_evaluation.md](05_experiments_and_evaluation.md).

## 5. Hợp đồng output analytics

Mỗi message trên `wikipedia-analytics` là một JSON object, key là tên metric.

| Trường | Có ở metric | Ý nghĩa |
|---|---|---|
| `metric` | cả ba | `edit_count`, `bot_ratio`, `activity_spike` |
| `window_start`, `window_end` | cả ba | Biên window theo event time, UTC |
| `counted_types` | cả ba | `all`, hoặc danh sách `type` được tính nếu đặt `ANALYTICS_EVENT_TYPES` |
| `emitted_at` | cả ba | Thời điểm Spark phát bản ghi |
| `wiki` | `edit_count`, `activity_spike` | Wiki project |
| `edit_count`, `bot_edits` | `edit_count` | Số event trong window và phần do bot |
| `total_events`, `bot_count`, `human_count`, `unknown_count` | `bot_ratio` | Mẫu số là `total_events` của cùng window |
| `bot_share`, `human_share`, `unknown_share` | `bot_ratio` | Làm tròn 4 chữ số, nên tổng ba tỷ lệ lệch khỏi 1 tối đa 1,5e-4 |
| `title`, `distinct_users`, `human_edits` | `activity_spike` | `distinct_users` tính bằng `approx_count_distinct` vì streaming aggregation không hỗ trợ `countDistinct` |
| `threshold_edits`, `threshold_distinct_users` | `activity_spike` | Ngưỡng đang áp dụng, phát kèm để báo cáo tái lập được |

`activity_spike` chỉ phát những window vượt ngưỡng. Đây là phát hiện đột biến hoạt động chỉnh sửa, không phải khẳng định có tranh chấp: xem mục giới hạn trong [05_experiments_and_evaluation.md](05_experiments_and_evaluation.md).

## 6. Guarantee theo từng chặng

Không tuyên bố exactly-once cho toàn pipeline. Từng chặng khác nhau:

| Chặng | Guarantee quan sát được | Cơ sở |
|---|---|---|
| SSE → Producer | At-least-once. Resume theo `Last-Event-ID` có thể gửi lại event đã gửi. | [07_ingestion.md](07_ingestion.md) |
| Producer → Kafka | Không mất và không trùng do retry trong một phiên, nhờ `acks=all` và idempotent producer. Không chống trùng do replay SSE. | [04_technology_stack.md](04_technology_stack.md) |
| Kafka → Spark | Offset được lưu trong checkpoint, nên restart đọc tiếp từ batch chưa commit. Một batch đang dở sẽ được xử lý lại. | Bài test recovery |
| Spark → Parquet | File sink ghi theo batch và commit qua `_spark_metadata`, nên batch chạy lại không tạo file trùng. | Bài test recovery |
| Spark → Kafka analytics | At-least-once. Kafka sink không dùng transaction, nên một batch chạy lại có thể phát lại message của window đó. | Dashboard phải chịu được bản ghi lặp theo `(metric, window_start)` |

## 7. Cấu hình và cổng

| Dịch vụ | Trong network docker | Từ máy host |
|---|---|---|
| Kafka | `kafka:9092` | `localhost:9094` |
| HDFS NameNode RPC | `namenode:8020` | — |
| HDFS NameNode UI | `namenode:9870` | `localhost:9870` |
| Spark driver UI | `spark:4040` | `localhost:4040` khi có job đang chạy |

`localhost` bên trong container không phải máy host, nên Kafka khai báo hai listener: `INTERNAL` cho client trong network và `HOST` cho client trên máy.

## 8. Giới hạn đã biết

- Một broker Kafka, một DataNode, replication 1. Không chứng minh được high availability.
- Spark `local[2]`: không có phân phối task nhiều node, nên số throughput đo được là của một máy.
- Hai nhánh query đọc Kafka độc lập, nên tổng lượng đọc gấp đôi lượng dữ liệu vào.
- `spark_processed_at` dùng `current_timestamp()`, nên mọi dòng trong một micro-batch có cùng giá trị. Latency producer-to-processing vì vậy phụ thuộc độ dài trigger.
