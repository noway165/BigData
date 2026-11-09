# Kịch bản demo kỹ thuật

Dành cho buổi bảo vệ. Chuẩn bị trước, demo trực tiếp phần pipeline và recovery; thí nghiệm watermark và latency thì trình bày bằng log và bảng đã lưu, không chạy lại trước lớp.

## Chuẩn bị trước buổi bảo vệ

| Việc | Lệnh / ghi chú |
|---|---|
| Tải sẵn jar connector vào cache Ivy | Chạy một lần `docker compose up -d && sh scripts/bootstrap.sh`, rồi `docker exec spark sh /opt/app/scripts/spark_analytics_selfcheck.sh`. Volume `spark_ivy` giữ jar, buổi demo không cần mạng để tải. |
| Pull sẵn image | `docker compose pull` |
| Chạy trước thí nghiệm watermark và recovery | Giữ nguyên thư mục trong `outputs/experiment_results/` để trình bày |
| Mở sẵn tab | Dashboard `http://localhost:8501`, Live feed `http://localhost:8080`, NameNode UI `http://localhost:9870`, Spark driver UI `http://localhost:4040` |
| Build sẵn image Dashboard | `docker compose --profile dashboard build` (cần mạng để cài Streamlit) |
| Lưu output analytics dự phòng | `sh scripts/capture_analytics.sh 5000` sau khi analytics đã chạy ~15 phút |
| Chạy producer sớm 10–15 phút | Để HDFS đã có vài partition giờ và Kafka đã có dữ liệu |

## Trình tự demo

**1. Khởi động hạ tầng**

Nhanh nhất là nhấp đúp `demo.bat` ở thư mục gốc: nó làm hết bước 1, 2, 4 và mở cả hai trang. Giữ cửa sổ đó mở suốt buổi; cuối buổi nhấn `Q` rồi `Y` để tắt.
Muốn vừa chạy vừa giải thích từng bước thì gõ tay:

```bash
docker compose up -d
```

```bash
sh scripts/bootstrap.sh
```

Chỉ ra: Kafka topic 3 partition, retention 24 giờ; thư mục `/checkpoints` trên HDFS.

**2. Producer đọc SSE và gửi vào Kafka**

```bash
docker compose --profile producer up -d && docker logs -f producer
```

Chỉ ra dòng thống kê: `received`, `sent`, `canary_skipped`, `reconnects`.

**3. Raw sink ghi Parquet xuống HDFS**

```bash
docker exec -d spark sh /opt/app/scripts/submit_raw_job.sh
```

```bash
docker exec namenode hdfs dfs -ls -R /data/raw/recent_changes
```

Chỉ ra partition `year=/month=/day=/hour=` theo event time, và `_spark_metadata`.

**4. Analytics chạy với event-time window và watermark**

```bash
docker exec -d spark sh /opt/app/scripts/submit_analytics_job.sh
```

```bash
sh scripts/consume_analytics.sh 10
```

Chỉ ra ba metric trên cùng một topic, biên window theo event time, và `threshold_edits` đi kèm bản ghi spike. Mở Spark UI tab Structured Streaming: ba query, mỗi query một checkpoint riêng.

**5. Dashboard** (Khoa trình bày)

```bash
docker compose --profile dashboard up -d
```

Mở `http://localhost:8501`. Chỉ lần lượt:

1. Hàng chỉ số trên cùng: số event của window 1 phút mới nhất, so với phút trước. Nhấn mạnh window theo **event time** và chỉ phát khi đã đóng, nên số liệu chậm hơn đồng hồ khoảng 2–3 phút — đó là cái giá của watermark 1 phút.
2. Biểu đồ số event mỗi phút, tách Bot / Human + Unknown; Top wiki (thường là `commonswiki`, `wikidatawiki`, `enwiki`).
3. Donut Bot / Human / Unknown: mẫu số là tổng event của cùng window 5 phút, kể cả Unknown.
4. Bảng cảnh báo spike: gọi là "hoạt động tăng đột biến", không phải edit war; chỉ cột *Do bot* và bật "Ẩn spike chủ yếu do bot" để thấy phần còn lại.
5. Mở khung "Độ trễ phát kết quả và tình trạng dữ liệu": độ trễ `emitted_at − window_end`, số bản ghi lặp đã khử.

Sau bước 6 (recovery), quay lại khung này: số "bản ghi lặp đã khử" có thể tăng — đó là at-least-once của Kafka sink, Dashboard đã xử lý.

Dự phòng nếu Kafka/Spark trục trặc: `ANALYTICS_SOURCE_FILE=outputs/experiment_results/analytics_capture.jsonl streamlit run src/dashboard/app.py` (file lưu trước bằng `sh scripts/capture_analytics.sh 5000`).

**5b. Live feed (`http://localhost:8080`)**

Cùng profile `dashboard`, nên lệnh ở bước 5 bật luôn cả hai. Trang này đọc **cả hai** topic: số liệu tổng hợp từ `wikipedia-analytics`, và log từng lượt sửa từ topic raw `wikipedia-recent-changes`.

Chỉ ra: hàng chỉ số cỡ lớn, hai biểu đồ SVG, và **log trực tiếp** — mỗi dòng là một lượt sửa thật với giờ, người sửa (đã che: `Sim***`), loại thay đổi, tên bài và số byte thay đổi. Log lọc `enwiki` và `viwiki` để đọc kịp; toàn bộ luồng là khoảng 36 sự kiện mỗi giây.

Trang không reload: nó gọi `/api/state` mỗi 2 giây nên không nháy và không nhảy về đầu trang. Không tải tài nguyên ngoài nào, mất mạng vẫn hiện.

Log chỉ có sự kiện **từ lúc mở trang** (consumer raw đọc từ `latest`), nên bật sớm vài phút.

**6. Crash recovery**

Phần ăn điểm. Nếu còn thời gian thì chạy rút gọn:

```bash
RECOVERY_RUN1_SECONDS=60 RECOVERY_GAP_SECONDS=45 RECOVERY_RUN2_SECONDS=90 sh scripts/crash_recovery_test.sh
```

Nếu không còn thời gian thì mở thư mục `outputs/experiment_results/recovery_<timestamp>/` và đối chiếu: offset Kafka tăng trong lúc Spark chết, batch đầu sau restart bắt đầu từ offset đã commit, không nhảy tới cuối topic.

**7. Bảng số liệu**

Latency p50/p95 ([latency_report.md](../outputs/experiment_results/latency_report.md)), so sánh watermark và recovery ([05_experiments_and_evaluation.md](05_experiments_and_evaluation.md)).

## Phần nói về kỹ thuật

- Vì sao có Kafka ở giữa: tách producer khỏi Spark, nên Spark chết thì dữ liệu vẫn vào Kafka và đọc tiếp được từ offset. Đây là điều kiện để demo recovery.
- Event time so với processing time: window dùng `event_time` từ `meta.dt`, nên event đến trễ vẫn vào đúng window của nó.
- Watermark: mốc Spark ngừng chờ event trễ, để đóng window và bỏ state. Watermark lớn giữ state lâu hơn nên tốn bộ nhớ hơn.
- Checkpoint lưu gì: offset Kafka đã lên kế hoạch và đã commit, metadata query và state của aggregation. Phải nằm trên HDFS, vì checkpoint trong filesystem container sẽ mất khi container bị xóa.
- Guarantee: không tuyên bố exactly-once toàn hệ thống. Nói theo từng chặng, bảng ở [02_architecture.md](02_architecture.md) mục 6.
- Nút thắt khi tăng tải: với Spark `local[2]` và một broker, giới hạn nằm ở số core của driver và `maxOffsetsPerTrigger`, không nằm ở Kafka.

## Nếu có sự cố trong lúc demo

| Hiện tượng | Xử lý |
|---|---|
| Submit hai job cùng lúc khi cache Ivy còn trống | Hai tiến trình `spark-submit` cùng resolve một package có thể chặn nhau và job treo không ghi gì. Submit job đầu, chờ nó chạy, rồi submit job thứ hai. Luôn redirect log (`> /opt/app/outputs/<ten>.log 2>&1`) để nhìn thấy khi nó treo. |
| `spark-submit` treo ở bước tải package | Jar đã nằm trong volume `spark_ivy`; kiểm tra `docker exec spark ls /tmp/ivy/jars`. Thiếu thì chạy lại một job trước đó để nạp cache. |
| Query lỗi ghi HDFS | Kiểm tra namenode còn sống: `docker exec namenode hdfs dfsadmin -report`. |
| Dashboard trống | Topic chưa có message nào, xem dòng dưới. Nếu topic có dữ liệu mà trang vẫn trống thì xem `docker logs dashboard`. |
| Topic analytics rỗng | Window chưa đóng. `outputMode("append")` chỉ phát khi window đóng, nên phải chờ qua `window + watermark`. |
| Producer không gửi | `docker logs producer`: nếu `reconnects` tăng liên tục thì là mạng hoặc rate limit của Wikimedia. |
| Hết RAM | Dừng nhánh raw (`docker exec spark pkill -f raw_to_hdfs.py`), demo nhánh analytics; hoặc giảm `ANALYTICS_METRICS` xuống một metric. |
