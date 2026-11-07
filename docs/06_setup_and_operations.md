# Setup và vận hành: phần ingestion

Tài liệu này hướng dẫn chạy và kiểm tra phần ingestion: producer, raw sink và cách đo. `docker-compose.yml`, service Kafka, Spark, HDFS và việc tạo topic do Giang phụ trách. Tên container và network dưới đây là chỗ giữ chỗ, cần điền theo `docker-compose.yml` của Giang khi file này có trong repo.

Các lệnh dưới đây viết cho Docker. Với Podman, thay `docker` bằng `podman`.

## 1. Hạ tầng phần ingestion cần

| Thành phần | Yêu cầu | Biến cấu hình |
|---|---|---|
| Kafka | Producer và Spark truy cập được | `KAFKA_BOOTSTRAP_SERVERS` |
| HDFS | NameNode truy cập được từ Spark | `RAW_OUTPUT_PATH`, `RAW_CHECKPOINT_PATH` |
| Spark | Có `spark-submit`, connector khớp phiên bản Spark và Scala | `SPARK_KAFKA_PACKAGE` |

Giá trị mặc định giả định hostname nội bộ `kafka:9092` và `namenode:8020`. Stack khác tên thì đổi qua biến môi trường, không cần sửa code.

Đã kiểm thử với Kafka 3.9.1 (KRaft), Hadoop 3.4.1 và Spark 3.5.6 (Scala 2.12). Đây chỉ là bộ dùng để test, phiên bản chính thức do Giang chốt. Nếu Spark khác 3.5.6 thì phải đặt `SPARK_KAFKA_PACKAGE` cho khớp.

## 2. Kiểm tra topic

Giang tạo topic. Kiểm tra topic đã tồn tại trước khi chạy producer:

```bash
docker exec <kafka-container> /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic wikipedia-recent-changes
```

Nếu cần tạo thủ công để test riêng phần ingestion:

```bash
docker exec <kafka-container> /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic wikipedia-recent-changes --partitions 3 --replication-factor 1 --config retention.ms=86400000
```

## 3. Chạy producer

Trong container (khuyến nghị, chạy từ thư mục gốc repo):

```bash
docker build -f src/producer/Dockerfile -t wikipedia-sse-producer .
```

```bash
docker run -d --name producer --network <network> -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 wikipedia-sse-producer
```

Hoặc trên máy host, nếu Kafka có listener cho host (mặc định `localhost:9094`):

```bash
pip install -r src/producer/requirements.txt
```

```bash
python src/producer/sse_kafka_producer.py
```

Xem log:

```bash
docker logs -f producer
```

Một dòng `stats` bình thường có `delivered` gần bằng `sent` và `queued` nhỏ:

```text
INFO producer stats {'received': 5464, 'sent': 5464, 'missing_event_id': 0, 'delivered': 5463} queued=1 source_to_producer n=1136 p50=0.269s p95=0.374s
```

Chỉ chạy một producer tại một thời điểm. Hai producer song song sẽ ghi trùng mọi event.

## 4. Kiểm tra topic nhận event

```bash
docker exec <kafka-container> /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:9092 --topic wikipedia-recent-changes
```

Tổng offset các partition phải tăng dần, và bằng `delivered` của producer nếu topic mới tạo và chỉ có một phiên producer.

## 5. Chạy raw sink

Mount repo vào container Spark tại `/opt/app` (hoặc đặt `APP_DIR`), rồi chạy:

```bash
docker exec -d <spark-container> sh /opt/app/scripts/submit_raw_job.sh
```

Lần đầu Spark tải connector nên cần Internet. File Parquet đầu tiên xuất hiện sau khoảng một phút.

Kiểm tra trên HDFS:

```bash
docker exec <namenode-container> hdfs dfs -ls -R /data/raw/recent_changes
```

File nằm ở dạng `year=2026/month=11/day=07/hour=17/part-*.parquet`. Có thể xem bằng giao diện web của NameNode, cổng 9870, mục Utilities, Browse the file system. Ảnh mẫu: [`outputs/experiment_results/hdfs_raw_parquet.png`](../outputs/experiment_results/hdfs_raw_parquet.png).

Trên Windows dùng Git Bash, thêm `MSYS_NO_PATHCONV=1` trước lệnh có đường dẫn HDFS, nếu không Git Bash sẽ đổi `/data/...` thành đường dẫn Windows.

Không xóa `/checkpoints/raw_to_hdfs` giữa các lần chạy, nếu không query sẽ đọc lại từ đầu topic và ghi trùng. Muốn chạy thí nghiệm mới thì dùng checkpoint và thư mục output mới.

Trong file Parquet có sẵn 3 cột thời gian để đo latency. Nếu query dừng (ví dụ Kafka tắt khoảng một phút), khởi động lại đúng lệnh trên là nó đọc tiếp từ checkpoint.

## 6. Báo cáo chất lượng dữ liệu và latency

```bash
docker exec <spark-container> sh /opt/app/scripts/quality_report.sh
```

Kết quả ghi vào `outputs/experiment_results/raw_quality_report.json`, gồm schema thực tế, trường thiếu, bản ghi trùng và p50/p95 của 3 loại latency. Ý nghĩa các trường xem ở [07_ingestion.md](07_ingestion.md).

Khảo sát schema trực tiếp từ Wikimedia, không cần Kafka (đối số là số giây chạy):

```bash
python scripts/inspect_sse_schema.py 60
```

## 7. Test

Unit test producer, không cần Kafka:

```bash
pip install -r src/producer/requirements.txt pytest
```

```bash
python -m pytest tests -q
```

Kiểm tra phép biến đổi Spark trên event mẫu, không cần Kafka hay HDFS:

```bash
docker exec <spark-container> sh /opt/app/scripts/spark_selfcheck.sh
```

## 8. Tài nguyên

Stack test gồm Kafka, NameNode, DataNode, producer và raw sink dùng khoảng 1,9 GB RAM (đo khi Kafka heap tối đa 512 MB, mỗi tiến trình HDFS 256 MB, Spark driver 768 MB). Máy test có 3,76 GB cho container. Thêm job analytics có thể thiếu RAM trên máy cấu hình tương tự, nên cần tính khi chốt stack chung.
