# Data contract

Tài liệu này là thỏa thuận dữ liệu giữa ingestion (Lâm), stream processing (Giang) và analytics (Khoa). Đổi tên hoặc kiểu của một trường ở đây thì phải báo cả nhóm.

## 1. Kafka topic

Giang tạo topic. Partition và retention dưới đây là giá trị Lâm đã dùng để test, cần khớp với cấu hình chung.

| Thuộc tính | Giá trị |
|---|---|
| Topic | `wikipedia-recent-changes` |
| Partition | 3 |
| Replication factor | 1 (một broker, môi trường lab) |
| Retention | 24 giờ |
| Key | `<wiki>:<title>`. Thiếu một trong hai thì dùng `event_id`. |
| Value | JSON UTF-8, gồm 4 trường ở mục 2 |

Key `wiki:title` đưa mọi sự kiện của một bài viết vào cùng partition, nên thứ tự theo bài được giữ. Nếu chỉ dùng `wiki` làm key, `commonswiki` và `wikidatawiki` sẽ dồn tải vào một partition.

## 2. Message value

```json
{
  "event_id": "1036cbea-4a21-43f2-9d59-104bb9e153d5",
  "event_time": "2026-11-07T16:20:17.814Z",
  "producer_received_at": "2026-11-07T16:20:18.120Z",
  "raw_value": "{\"$schema\":\"/mediawiki/recentchange/1.0.0\", ...}"
}
```

| Trường | Kiểu | Nguồn | Có thể null |
|---|---|---|---|
| `event_id` | string | `meta.id` | Có, nếu event thiếu |
| `event_time` | timestamp UTC, ISO-8601 | `meta.dt` | Có, nếu event thiếu |
| `producer_received_at` | timestamp UTC, ISO-8601 | Đồng hồ producer khi nhận xong event | Không |
| `raw_value` | string | JSON gốc, giữ nguyên từng ký tự | Không |

Cách parse mẫu nằm ở `to_raw_rows()` trong [`src/streaming/raw_to_hdfs.py`](../src/streaming/raw_to_hdfs.py). Lưu ý `raw_value` là chuỗi JSON nằm trong JSON, nên phải parse hai lần: một lần cho message, một lần cho `raw_value`.

## 3. Trường nghiệp vụ trong `raw_value`

Khảo sát 1.882 event thật trong 60 giây ngày 2026-11-08, bằng [`scripts/inspect_sse_schema.py`](../scripts/inspect_sse_schema.py). Kết quả đầy đủ ở [`outputs/experiment_results/sse_schema_survey.txt`](../outputs/experiment_results/sse_schema_survey.txt), event mẫu ở [`tests/sample_events/`](../tests/sample_events/).

"Có ở mọi event" chỉ nghĩa là chưa thấy thiếu trong mẫu này. Code không được giả định trường nào luôn có.

| Trường | Kiểu | Quan sát |
|---|---|---|
| `wiki` | string | Có ở mọi event |
| `title` | string | Có ở mọi event |
| `type` | string | `edit` 1117, `categorize` 620, `new` 82, `log` 63 |
| `namespace` | int | Có ở mọi event |
| `user` | string | Có ở mọi event |
| `bot` | boolean | `true` 799, `false` 1083, không thấy null hay thiếu |
| `timestamp` | long, giây Unix | Có ở mọi event |
| `meta.dt` | string ISO-8601 | Có ở mọi event |
| `meta.id` | string (UUID) | Có ở mọi event |
| `id` | long | Thiếu ở 22/1882, nên không dùng làm khóa |
| `length`, `revision`, `minor` | | Thiếu ở 683/1882, chỉ thấy ở `edit` và `new` |
| `patrolled` | boolean | Thiếu ở 1109/1882 |
| `log_*` | | Chỉ có ở 63 event `log` |

## 4. Quy tắc

- **Event time:** window và watermark dùng `event_time` (`meta.dt`). Trong một mẫu 448 event, `meta.dt` đi sau `timestamp` của MediaWiki: p50 1,24 giây, p95 3,19 giây, lớn nhất 9,51 giây. Vì vậy `event_time` là thời điểm Event Platform ghi nhận, không phải đúng lúc người dùng bấm lưu.
- **Bot:** `bot = true` là Bot, `false` là Human, null hoặc thiếu là Unknown. Không đổi null thành `false`.
- **Loại sự kiện:** topic chứa mọi `type`, gồm cả `categorize` và `log`. Mẫu cho thấy `categorize` chiếm khoảng một phần ba, nên Khoa cần chốt metric "edit count" chỉ tính `edit` và `new` hay tính tất cả. Nếu tính tất cả, số liệu sẽ lớn hơn số lượt sửa thật.
- **Canary:** event có `meta.domain = "canary"` là event giám sát của Wikimedia. Producer bỏ qua và đếm ở `canary_skipped`.
- **Trùng lặp:** kiểm tra theo `event_id`. Có thể xuất hiện trùng khi resume SSE (xem [07_ingestion.md](07_ingestion.md)).
- **Riêng tư:** `user` có trong raw để đếm `distinct_users`. Không đưa tên tài khoản lên Dashboard.
- **Log của Live feed:** log từng lượt sửa ở trang Live feed (`src/dashboard/dashboard.py`) chỉ hiện tên đã che, không hiện tên thật, nên không trái quy tắc trên. Che nghĩa là: IPv4 giữ hai nhóm đầu (`171.224.3.9` thành `171.224.*.*`); IPv6 giữ hai nhóm đầu; tên tài khoản giữ 3 ký tự đầu (`Printstream` thành `Pri***`), tên từ 3 ký tự trở xuống giữ 1 ký tự rồi thêm `***`; thiếu tên hiện `ẩn danh`. Log cũng không bao giờ hiện trường `comment` vì đó là văn bản tự do của người lạ và trang được chiếu trước lớp. Các khối tổng hợp không chứa tên người dùng. Che chỉ giảm nhận diện, không phải ẩn danh hóa: tên trong raw và Parquet vẫn là tên thật.

## 5. Raw Parquet trên HDFS

Đường dẫn: `hdfs://namenode:8020/data/raw/recent_changes/year=YYYY/month=MM/day=DD/hour=HH/`, theo giờ UTC của `event_time`.

| Cột | Kiểu |
|---|---|
| `event_id` | string |
| `event_time`, `producer_received_at`, `spark_processed_at` | timestamp |
| `wiki`, `title`, `type`, `user` | string |
| `namespace` | int |
| `bot` | boolean |
| `timestamp` | long |
| `raw_value` | string |
| `kafka_partition` | int |
| `kafka_offset` | long |
| `year`, `month`, `day`, `hour` | Cột partition. Khi đọc lại, Spark tự suy ra kiểu int, nên `day=07` thành `7`. |

Message Kafka hỏng vẫn được ghi, với các cột parse bị null. Nhờ vậy, báo cáo chất lượng đếm được dòng hỏng thay vì mất dấu chúng.

`spark_processed_at` dùng `current_timestamp()`, nên mọi dòng trong một micro-batch có cùng giá trị (thời điểm batch bắt đầu).

## 6. Ba loại latency

| Chỉ số | Công thức |
|---|---|
| Source-to-producer | `producer_received_at - event_time` |
| Producer-to-processing | `spark_processed_at - producer_received_at` |
| End-to-end observed | `spark_processed_at - event_time` |

Cả ba do `quality_report.py` tính từ raw Parquet. Kết quả và giới hạn đo nằm ở [`outputs/experiment_results/latency_report.md`](../outputs/experiment_results/latency_report.md).
