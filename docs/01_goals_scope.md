# Mục tiêu, phạm vi và định nghĩa metric

**Đề tài:** Xây dựng hệ thống phân tích hoạt động chỉnh sửa Wikipedia theo thời gian thực sử dụng Kafka và Spark Structured Streaming.

**Nhóm:** Giang (trưởng nhóm, stream processing), Lâm (ingestion, storage, latency), Khoa (analytics, Dashboard, báo cáo).

## 1. Bài toán

Mỗi lần có người sửa một trang Wikipedia (hay Wikidata, Commons…), Wikimedia phát một sự kiện công khai qua EventStreams (`stream.wikimedia.org/v2/stream/recentchange`, giao thức SSE). Luồng này liên tục, không cần API key; khảo sát của nhóm đếm được 1.882 event trong 60 giây, khoảng 30 event/giây ([sse_schema_survey.txt](../outputs/experiment_results/sse_schema_survey.txt)).

Đồ án đưa luồng này qua một pipeline streaming hoàn chỉnh và trả lời bằng số đo ba câu hỏi:

1. Dữ liệu đến hệ thống trễ bao nhiêu so với lúc sự kiện xảy ra?
2. Nên chọn watermark bao nhiêu để cân bằng giữa độ đầy đủ, bộ nhớ state và độ trễ?
3. Pipeline có sống sót sau khi Spark chết đột ngột không, và có mất hay trùng dữ liệu không?

## 2. Mục tiêu

| # | Mục tiêu | Bằng chứng |
|---|---|---|
| 1 | Thu thập SSE, gửi vào Kafka, tự kết nối lại khi rớt mạng | `src/producer/`, [ingestion_verification.md](../outputs/experiment_results/ingestion_verification.md) |
| 2 | Xử lý theo event time bằng Spark Structured Streaming, có window và watermark | `src/streaming/analytics_job.py` |
| 3 | Lưu raw vào HDFS dạng Parquet, chia partition theo giờ | `src/streaming/raw_to_hdfs.py`, [hdfs_raw_parquet.png](../outputs/experiment_results/hdfs_raw_parquet.png) |
| 4 | Tính ba metric nghiệp vụ (mục 4) | topic `wikipedia-analytics` |
| 5 | Hiển thị trên Dashboard cập nhật liên tục | `src/dashboard/app.py` |
| 6 | Đo latency p50/p95, so sánh watermark, thử crash recovery | [05_experiments_and_evaluation.md](05_experiments_and_evaluation.md) |

## 3. Phạm vi

**Làm:** một pipeline chạy được trên một máy bằng Docker Compose (Podman), demo được từ đầu đến cuối, có số đo và giải thích được.

**Không làm:**

- Không phân tích nội dung văn bản (NLP), không đọc diff của lượt sửa.
- Không khẳng định phát hiện được tranh chấp hay động cơ của người sửa. Metric spike chỉ là **đột biến hoạt động**.
- Không xây mô hình Machine Learning.
- Không làm cluster nhiều node hay high availability: 1 broker Kafka, 1 NameNode, 1 DataNode, Spark `local[2]`.
- Không tuyên bố exactly-once cho toàn pipeline (xem [02_architecture.md](02_architecture.md) mục 6).

## 4. Định nghĩa metric

Ba metric được tính trong `analytics_job.py`, mỗi metric một streaming query và một checkpoint riêng, cùng ghi vào topic `wikipedia-analytics` với key là tên metric.

Quy ước chung:

- **Thời gian:** mọi window tính theo `event_time` = `meta.dt` (thời điểm Event Platform ghi nhận sự kiện), giờ UTC. Event không có `event_time` không thuộc window nào nên bị loại khỏi analytics; báo cáo chất lượng vẫn đếm chúng trên raw Parquet.
- **Watermark:** 1 phút, chọn từ thí nghiệm (mục 1 của [05](05_experiments_and_evaluation.md)).
- **Output mode:** `append`. Mỗi window chỉ được phát một lần, khi đã đóng (event time vượt `window_end + watermark`). Vì vậy Dashboard chậm hơn thời gian thực khoảng `độ dài window + watermark + trigger`.
- **Loại sự kiện:** topic raw chứa mọi `type`: `edit`, `new`, `categorize`, `log`. Trong mẫu khảo sát, `categorize` chiếm khoảng 1/3. Mặc định analytics đếm **tất cả** (`counted_types = all`), và Dashboard ghi rõ điều đó. Muốn đúng nghĩa "lượt sửa" thì chạy analytics với `ANALYTICS_EVENT_TYPES=edit,new`; trường `counted_types` trong từng bản ghi output cho biết lần chạy đã chọn gì.

### 4.1. Edit count theo wiki

| Mục | Giá trị |
|---|---|
| Câu hỏi | Wiki project nào đang được sửa nhiều nhất? Tổng hoạt động thay đổi thế nào theo thời gian? |
| Nhóm theo | `wiki`, tumbling window 1 phút |
| Output | `window_start`, `window_end`, `wiki`, `edit_count`, `bot_edits` |
| Dashboard | Tổng event của window mới nhất (so với phút trước); biểu đồ vùng số event mỗi phút, tách Bot và phần còn lại; Top wiki trong N phút gần nhất |

### 4.2. Bot / Human / Unknown

| Mục | Giá trị |
|---|---|
| Câu hỏi | Bao nhiêu phần hoạt động là của bot? |
| Phân loại | `bot = true` → **Bot**; `bot = false` → **Human**; `bot` null hoặc thiếu → **Unknown**. Không đổi null thành `false`. |
| Nhóm theo | tumbling window 5 phút |
| Mẫu số của tỷ lệ | `total_events` của **cùng window**, kể cả Unknown. Ba tỷ lệ cộng lại bằng 1. |
| Output | `total_events`, `bot_count`, `human_count`, `unknown_count`, `bot_share`, `human_share`, `unknown_share` |
| Dashboard | Biểu đồ donut của window 5 phút mới nhất; đường tỷ lệ qua các window |

"Human" ở đây nghĩa là tài khoản không mang cờ bot. Wikipedia chỉ gắn cờ bot cho tài khoản đã đăng ký là bot, nên script chạy bằng tài khoản thường vẫn bị tính là Human. Trong mẫu khảo sát, `bot` không bao giờ thiếu, nên Unknown thường bằng 0; nhóm vẫn giữ nhóm này vì không được giả định trường luôn có.

### 4.3. Edit Activity Spike (Potential Edit War)

| Mục | Giá trị |
|---|---|
| Câu hỏi | Bài viết nào đang bị sửa dồn dập bất thường? |
| Nhóm theo | (`wiki`, `title`), sliding window 2 phút, trượt 30 giây |
| Điều kiện | `edit_count > 10` (`SPIKE_MIN_EDITS`). Điều kiện thử nghiệm thứ hai: `distinct_users > N` (`SPIKE_MIN_DISTINCT_USERS`, mặc định tắt) |
| Output | `wiki`, `title`, `edit_count`, `distinct_users` (gần đúng, `approx_count_distinct`), `bot_edits`, `human_edits`, ngưỡng đã dùng |
| Dashboard | Bảng cảnh báo, mỗi bài một dòng (gộp các window trượt chồng nhau, giữ window có nhiều lượt sửa nhất); cột *Do bot* khi hơn nửa số lượt sửa là bot; tùy chọn ẩn các dòng đó |

Đây là **heuristic phát hiện đột biến hoạt động**, không phải bộ phát hiện edit war:

- Một bot sửa hàng loạt (ví dụ trên Wikidata) vượt ngưỡng dễ dàng mà không có tranh chấp nào. Vì vậy output tách `bot_edits` và `human_edits`.
- Một sự kiện nóng ngoài đời (bầu cử, thể thao, người nổi tiếng qua đời) khiến nhiều người cùng cập nhật một bài; đó là hoạt động hợp tác, không phải tranh chấp.
- Một người tự sửa nhiều lần liên tiếp cũng vượt ngưỡng; điều kiện `distinct_users` để giảm trường hợp này.
- Edit war thật cần xem nội dung: các lượt hoàn tác (revert) qua lại giữa hai phía. Đồ án không đọc nội dung, nên không kết luận được.

Một sliding window 2 phút trượt 30 giây nghĩa là mỗi event nằm trong 4 window. Một đợt sửa dồn dập vì vậy thường xuất hiện trong nhiều bản ghi output; Dashboard gộp lại theo bài để bảng không lặp.

### 4.4. Độ trễ phát kết quả (trên Dashboard)

Dashboard hiển thị `emitted_at − window_end` theo từng metric: window đóng xong bao lâu thì kết quả tới Kafka. Con số này gồm thời gian chờ watermark và chờ trigger, và **khác** với ba loại latency từng event (source-to-producer, producer-to-processing, end-to-end) đo trên raw Parquet ([latency_report.md](../outputs/experiment_results/latency_report.md)).

## 5. Riêng tư

Raw Parquet giữ trường `user` để đếm `distinct_users`. Output analytics và Dashboard không chứa tên tài khoản nào; chỉ có số tổng hợp, tên wiki và tên bài viết (vốn công khai).

## 6. Câu hỏi phân tích và nơi trả lời

| Câu hỏi | Trả lời ở đâu |
|---|---|
| Wiki nào hoạt động nhiều nhất lúc này? | Dashboard: Top wiki |
| Bot chiếm bao nhiêu phần hoạt động? | Dashboard: Bot / Human / Unknown |
| Bài nào đang bị sửa dồn dập? Do người hay do bot? | Dashboard: bảng cảnh báo spike |
| Dữ liệu trễ bao nhiêu? | [latency_report.md](../outputs/experiment_results/latency_report.md) |
| Chọn watermark nào, vì sao? | [05](05_experiments_and_evaluation.md) mục 1 |
| Spark chết thì có mất dữ liệu không? | [05](05_experiments_and_evaluation.md) mục 2 |
