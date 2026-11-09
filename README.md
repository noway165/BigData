# Real-time Analytics for Wikipedia Recent Changes

Đồ án Big Data: phân tích hoạt động chỉnh sửa Wikipedia theo thời gian thực bằng Kafka và Spark Structured Streaming.

```text
Wikimedia EventStreams (SSE)
          |
          v
Python SSE Producer
          |
          v
Kafka: wikipedia-recent-changes
          |
          v
Spark Structured Streaming
          |
          +------------------------------+
          |                              |
          v                              v
Raw Data Query                    Analytics Query
          |                              |
          v                              v
HDFS / Parquet                  Kafka: wikipedia-analytics
                                         |
                                         v
                                 Dashboard HTTP (src/dashboard)
```

## Trạng thái

| Phần | Người phụ trách | Trạng thái trong nhánh này |
|---|---|---|
| Producer, raw sink, đo latency, tài liệu setup | Lâm | Có, đã chạy thử trên stack test cục bộ |
| `docker-compose.yml`, topic, Spark core, analytics query, recovery | Giang | Có, đã chạy end-to-end; có số liệu watermark và recovery |
| Dashboard, slide báo cáo | Khoa | Slide chưa có. Dashboard: có bản tối thiểu `src/dashboard/dashboard.py` để demo được ngay, thay bằng bản của Khoa khi xong |

## Khởi động

Toàn bộ hạ tầng nằm trong [`docker-compose.yml`](docker-compose.yml): Kafka (KRaft), HDFS, Spark. Hướng dẫn đầy đủ và cách kiểm tra từng bước ở [`docs/06_setup_and_operations.md`](docs/06_setup_and_operations.md), kiến trúc và lý do chọn cấu hình ở [`docs/02_architecture.md`](docs/02_architecture.md).

```bash
docker compose up -d
```

Nhóm đang chạy bằng **Podman** (`podman compose up -d`). Các script trong `scripts/` gọi `docker`, nên với Podman thì chạy kèm biến `DOCKER=podman`, ví dụ `DOCKER=podman sh scripts/bootstrap.sh`.

Tạo topic và thư mục HDFS, chạy lại được nhiều lần:

```bash
sh scripts/bootstrap.sh
```

Chạy producer:

```bash
docker compose --profile producer up -d
```

Chạy raw sink (ghi Parquet xuống HDFS):

```bash
docker exec -d spark sh /opt/app/scripts/submit_raw_job.sh
```

Chạy analytics (event-time window, watermark, ghi vào topic `wikipedia-analytics`):

```bash
docker exec -d spark sh /opt/app/scripts/submit_analytics_job.sh
```

Chạy dashboard (đọc topic `wikipedia-analytics`, mở http://localhost:8080):

```bash
docker compose --profile dashboard up -d
```

Kiểm tra Parquet trên HDFS:

```bash
docker exec namenode hdfs dfs -ls -R /data/raw/recent_changes
```

Kiểm tra output analytics:

```bash
sh scripts/consume_analytics.sh 10
```

Xuất báo cáo chất lượng dữ liệu và latency:

```bash
docker exec spark sh /opt/app/scripts/quality_report.sh
```

Cấu hình qua biến môi trường, danh sách ở [`.env.example`](.env.example). Không commit file `.env` thật.

## Thí nghiệm

So sánh watermark 10 giây / 1 phút / 5 phút trên cùng một mẫu dữ liệu được replay:

```bash
sh scripts/watermark_experiment.sh
```

Phục hồi sau sự cố: chạy ổn định, `kill -9` driver Spark, khởi động lại với checkpoint cũ:

```bash
sh scripts/crash_recovery_test.sh
```

Cả hai script ghi bằng chứng vào `outputs/experiment_results/`. Cách đọc kết quả và những gì chưa chứng minh được nằm ở [`docs/05_experiments_and_evaluation.md`](docs/05_experiments_and_evaluation.md).

## Kiểm thử

```bash
pip install -r src/producer/requirements.txt pytest
```

```bash
python -m pytest tests -q
```

Kiểm tra phép biến đổi Spark trên event mẫu:

```bash
docker exec spark sh /opt/app/scripts/spark_selfcheck.sh
```

Kiểm tra ba metric analytics (window, ngưỡng spike, phân loại Bot/Human/Unknown):

```bash
docker exec spark sh /opt/app/scripts/spark_analytics_selfcheck.sh
```

## Kết quả đã đo

Đo trên một máy, stack test cục bộ, trigger raw sink 30 giây, 17.249 event. Giới hạn đo xem trong [báo cáo latency](outputs/experiment_results/latency_report.md).

| Latency | p50 (giây) | p95 (giây) |
|---|---|---|
| Source-to-producer | 0,303 | 0,407 |
| Producer-to-processing | 14,858 | 29,149 |
| End-to-end observed | 15,178 | 29,457 |

Qua một lần ngắt mạng producer 90 giây và tắt Kafka 60 giây, số event ở producer, Kafka và Parquet đều khớp ở 34.061, không có dòng trùng. Chi tiết và những gì chưa kiểm chứng nằm ở [`ingestion_verification.md`](outputs/experiment_results/ingestion_verification.md).

Phần stream processing, đo ngày 2026-11-07 trên Podman 6.1.4 (WSL2, 7,6 GB cho container), Spark `local[2]`. Chi tiết và giới hạn ở [`docs/05_experiments_and_evaluation.md`](docs/05_experiments_and_evaluation.md).

| Watermark | Dòng bị loại / 20.000 event | State rows lớn nhất | Batch duration p50 |
|---|---|---|---|
| 10 giây | 118 | 168 | 1068 ms |
| 1 phút | 5 | 233 | 1288 ms |
| 5 phút | 0 | 488 | 1400 ms |

Crash recovery: `kill -9` driver Spark, 60 giây sau khởi động lại cùng checkpoint. Batch đầu sau restart là batch 25, tiếp số của phase 1, bắt đầu từ offset 16421/14672/14679 thay vì nhảy tới cuối topic (17155/15368/15346), nên không bỏ qua 2.084 event của khoảng chết.

## Tài liệu

| File | Nội dung |
|---|---|
| [`docs/02_architecture.md`](docs/02_architecture.md) | Thành phần, các streaming query, checkpoint, watermark, guarantee từng chặng |
| [`docs/03_data_contract.md`](docs/03_data_contract.md) | Message Kafka, schema thực tế, raw Parquet, định nghĩa latency |
| [`docs/04_technology_stack.md`](docs/04_technology_stack.md) | Phiên bản đã test và lý do chọn công nghệ |
| [`docs/06_setup_and_operations.md`](docs/06_setup_and_operations.md) | Lệnh chạy và kiểm tra từng thành phần |
| [`docs/05_experiments_and_evaluation.md`](docs/05_experiments_and_evaluation.md) | Thí nghiệm watermark và recovery: cách chạy, cách đọc, giới hạn |
| [`docs/07_ingestion.md`](docs/07_ingestion.md) | Xử lý reconnect, lỗi gửi và giới hạn đã biết |
| [`docs/08_demo_script.md`](docs/08_demo_script.md) | Kịch bản demo bảo vệ và cách xử lý sự cố tại chỗ |

## Cấu trúc thư mục

```text
docker-compose.yml  Kafka (KRaft), HDFS, Spark, producer
config/             Cấu hình Hadoop cho container và cho client Spark
src/producer/       SSE producer, cấu hình, Dockerfile
src/streaming/      Raw sink lên HDFS, analytics query, báo cáo chất lượng và latency
scripts/            Bootstrap, submit Spark job, thí nghiệm watermark và recovery
tests/              Unit test, kiểm tra Spark, event mẫu
docs/               Tài liệu
outputs/            Kết quả đo và bằng chứng kiểm thử
```
