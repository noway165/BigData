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
                                 Streamlit Dashboard
```

## Trạng thái

| Phần | Người phụ trách | Trạng thái trong nhánh này |
|---|---|---|
| Producer, raw sink, đo latency, tài liệu setup | Lâm | Có, đã chạy thử trên stack test cục bộ |
| `docker-compose.yml`, topic, Spark core, recovery | Giang | Chưa có |
| Analytics, Dashboard, slide | Khoa | Chưa có |

Khi có `docker-compose.yml`, mục khởi động dưới đây cần cập nhật theo file đó và chạy kiểm tra lại.

## Khởi động phần ingestion

Cần có sẵn Kafka đã tạo topic `wikipedia-recent-changes`, HDFS và Spark. Hướng dẫn đầy đủ, cả cách kiểm tra từng bước, nằm ở [`docs/06_setup_and_operations.md`](docs/06_setup_and_operations.md).

Build producer (chạy từ thư mục gốc repo):

```bash
docker build -f src/producer/Dockerfile -t wikipedia-sse-producer .
```

Chạy producer trong cùng network với Kafka:

```bash
docker run -d --name producer --network <network> -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 wikipedia-sse-producer
```

Chạy raw sink trong container Spark, với repo được mount tại `/opt/app`:

```bash
docker exec -d <spark-container> sh /opt/app/scripts/submit_raw_job.sh
```

Kiểm tra Parquet trên HDFS:

```bash
docker exec <namenode-container> hdfs dfs -ls -R /data/raw/recent_changes
```

Xuất báo cáo chất lượng dữ liệu và latency:

```bash
docker exec <spark-container> sh /opt/app/scripts/quality_report.sh
```

Cấu hình qua biến môi trường, danh sách ở [`.env.example`](.env.example). Không commit file `.env` thật.

## Kiểm thử

```bash
pip install -r src/producer/requirements.txt pytest
```

```bash
python -m pytest tests -q
```

Kiểm tra phép biến đổi Spark trên event mẫu:

```bash
docker exec <spark-container> sh /opt/app/scripts/spark_selfcheck.sh
```

## Kết quả đã đo

Đo trên một máy, stack test cục bộ, trigger raw sink 30 giây, 17.249 event. Giới hạn đo xem trong [báo cáo latency](outputs/experiment_results/latency_report.md).

| Latency | p50 (giây) | p95 (giây) |
|---|---|---|
| Source-to-producer | 0,303 | 0,407 |
| Producer-to-processing | 14,858 | 29,149 |
| End-to-end observed | 15,178 | 29,457 |

Qua một lần ngắt mạng producer 90 giây và tắt Kafka 60 giây, số event ở producer, Kafka và Parquet đều khớp ở 34.061, không có dòng trùng. Chi tiết và những gì chưa kiểm chứng nằm ở [`ingestion_verification.md`](outputs/experiment_results/ingestion_verification.md).

## Tài liệu

| File | Nội dung |
|---|---|
| [`docs/03_data_contract.md`](docs/03_data_contract.md) | Message Kafka, schema thực tế, raw Parquet, định nghĩa latency |
| [`docs/04_technology_stack.md`](docs/04_technology_stack.md) | Phiên bản đã test và lý do chọn công nghệ |
| [`docs/06_setup_and_operations.md`](docs/06_setup_and_operations.md) | Lệnh chạy và kiểm tra từng thành phần |
| [`docs/07_ingestion.md`](docs/07_ingestion.md) | Xử lý reconnect, lỗi gửi và giới hạn đã biết |

## Cấu trúc thư mục

```text
src/producer/       SSE producer, cấu hình, Dockerfile
src/streaming/      Raw sink lên HDFS, báo cáo chất lượng và latency
scripts/            Lệnh chạy Spark job, khảo sát schema
tests/              Unit test, kiểm tra Spark, event mẫu
docs/               Tài liệu
outputs/            Kết quả đo và bằng chứng kiểm thử
```
