# Công nghệ sử dụng

Bảng dưới là phiên bản chính thức, đã chốt trong [`docker-compose.yml`](../docker-compose.yml) và trong `SPARK_KAFKA_PACKAGE` của `scripts/submit_*.sh`. Đổi một dòng ở đây thì phải sửa cả hai nơi đó.

| Thành phần | Phiên bản đã test | Vai trò |
|---|---|---|
| Python | 3.12 | Producer |
| `confluent-kafka` | 2.16.1 | Kafka client của producer |
| Apache Kafka | 3.9.1 (KRaft) | Lớp đệm giữa ingestion và processing |
| Apache Spark | 3.5.6 (Scala 2.12) | Structured Streaming |
| `spark-sql-kafka-0-10_2.12` | 3.5.6 | Connector Spark đọc Kafka, phải khớp phiên bản Spark và Scala |
| Apache Hadoop (HDFS) | 3.4.1 | Lưu raw Parquet |

## Vì sao chọn

| Lựa chọn | Lý do | Đánh đổi |
|---|---|---|
| Kafka giữa producer và Spark | Spark dừng thì producer vẫn gửi được, Spark chạy lại thì đọc tiếp từ offset. Nhiều query đọc chung một topic. | Thêm một dịch vụ cần vận hành và tốn RAM |
| KRaft | Không cần thêm ZooKeeper, nên ít hơn một container | |
| `confluent-kafka` | Có delivery report, idempotent producer và retry sẵn | Là binding của thư viện C `librdkafka` |
| SSE đọc bằng `urllib` | Thư viện chuẩn đủ cho định dạng SSE, không thêm dependency | Tự viết phần parse SSE, có unit test |
| Parquet thay vì JSON | Lưu theo cột nên đọc vài cột không phải quét toàn bộ, có schema và nén | Không đọc trực tiếp bằng mắt được |
| Partition theo năm/tháng/ngày/giờ | Lọc theo thời gian chỉ đọc đúng thư mục cần | Trigger ngắn tạo nhiều file nhỏ |
| Key Kafka `wiki:title` | Giữ thứ tự theo bài viết, phân tải đều hơn key `wiki` | Tải vẫn lệch nếu một bài viết được sửa rất nhiều |
| `acks=all`, idempotence | Không mất và không trùng do retry trong một phiên producer | Không chống trùng do replay từ SSE |

## Cấu hình máy đo

| Mục | Giá trị |
|---|---|
| Máy | Windows 11, 7,36 GB RAM |
| Container runtime | Podman 6.0.2 chạy trên WSL2, 3,76 GB RAM khả dụng cho container |
| Dịch vụ | 1 Kafka broker, 1 NameNode, 1 DataNode, Spark `local[2]` |
