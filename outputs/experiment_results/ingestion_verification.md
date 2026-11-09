# Bằng chứng kiểm thử phần ingestion

Các kịch bản sự cố chạy ngày 2026-11-07 (UTC) trên stack test cục bộ của Lâm: Kafka 3.9.1, Hadoop 3.4.1, Spark 3.5.6, Podman. Sau đó phần ingestion được chạy lại trên `docker-compose.yml` của nhóm, xem mục cuối.

Số liệu gốc: [`raw_quality_report.json`](raw_quality_report.json), [`producer_run.log`](producer_run.log). Ảnh HDFS: [`hdfs_raw_parquet.png`](hdfs_raw_parquet.png).

## Kịch bản

| Thời điểm (UTC) | Sự kiện |
|---|---|
| 17:14:37 | Khởi động stack với dữ liệu và checkpoint mới |
| 17:24:20 đến 17:25:51 | Ngắt mạng producer khoảng 90 giây |
| 17:27:22 đến 17:28:24 | Tắt Kafka khoảng 60 giây, producer vẫn chạy |
| 17:29:11 | Raw query đã chết do Kafka tắt, tạo lại container với checkpoint cũ |
| 17:30:58 | Dừng producer bằng SIGTERM |

## Đối chiếu số lượng

| Nơi đếm | Số event |
|---|---|
| Producer `delivered` | 34.061 |
| Kafka, tổng end offset 3 partition (12.048 + 11.081 + 10.932) | 34.061 |
| Parquet trên HDFS, đọc bằng Spark | 34.061 |
| Dòng trùng `event_id` | 0 |

## Kết quả theo tình huống

| Tình huống | Quan sát |
|---|---|
| Ngắt mạng producer | Producer tự kết nối lại và gửi `Last-Event-ID`. Log `SSE gap closed gap_s=42.4 resumed_from_last_id=True`. Thời gian phát hiện chậm hơn lúc đứt thật nên `gap_s` nhỏ hơn 90 giây. |
| Tắt Kafka | Producer giữ message trong hàng đợi, không có `delivery failed`, gửi hết sau khi Kafka bật lại. |
| Raw query khi Kafka tắt | Query dừng với `Exited (1)`. Tạo lại với checkpoint cũ thì đọc tiếp và không tạo dòng trùng. |
| Dừng producer | `undelivered=0`, exit code 0. |
| Hở offset nguồn | `missing` = 1 ở `codfw`, 0 ở `eqiad`. Producer đã bỏ 2 event canary, mà canary vẫn chiếm offset, nên lỗ này nhiều khả năng là canary. Báo cáo không ghi offset của event bị bỏ, nên chưa khẳng định chắc chắn. |
| HDFS | 27 file Parquet, 14,4 MB, replication 1, `fsck` báo `HEALTHY`, 0 block lỗi. |
| Chất lượng dữ liệu | Không thiếu trường nào trong `missing_by_field`. |

## Khi producer tắt hẳn

Trong một lần chạy khác, producer tắt khoảng 3 phút rồi chạy lại mà không có `Last-Event-ID`. Báo cáo ghi nhận `missing` = 6.187, đúng với hành vi đã ghi trong [07_ingestion.md](../../docs/07_ingestion.md): event phát ra trong lúc producer tắt không lấy lại được. Lần chạy đó cũng vô tình có hai producer song song và tạo đúng 6.025 dòng trùng, cho thấy `duplicate_event_id_rows` phát hiện được trùng lặp thật.

## Chưa kiểm chứng

- Chạy trên Docker thật. Toàn bộ đã chạy bằng Podman.
- Bài crash recovery cho raw sink. Bài của Giang chạy trên query analytics. Mục raw query ở trên chỉ là một lần restart sau khi Kafka tắt, chưa lặp lại và chưa kill đột ngột.
- Dừng producer bằng Ctrl+C trên Windows. Tín hiệu dừng chỉ được test trong container Linux.
- Server Wikimedia tự đóng stream. Không xảy ra trong các lần chạy, chỉ được unit test.

## Chạy lại trên `docker-compose.yml` của nhóm

Chạy ngày 2026-11-09 (UTC) theo đúng lệnh trong README: `compose up`, `bootstrap.sh`, producer, `submit_raw_job.sh`, `quality_report.sh`. Số liệu gốc: [`raw_quality_report_team_stack.json`](raw_quality_report_team_stack.json). Lần này không tạo sự cố.

| Nơi đếm | Số event |
|---|---|
| Producer `delivered` | 13.778 |
| Kafka, tổng end offset 3 partition (4.485 + 4.641 + 4.652) | 13.778 |
| Parquet trên HDFS, đọc bằng Spark | 13.778 |
| Dòng trùng `event_id` | 0 |
| Hở offset nguồn (`missing`) | 0 |

HDFS có 9 file Parquet, 5,6 MB, replication 1, `fsck` báo `HEALTHY`. Producer dừng với exit code 0, `undelivered=0`.

Không dùng latency của lần chạy này để báo cáo: raw job được bật sau producer và phải tải connector, nên có backlog lúc đầu (p95 producer-to-processing 129 giây). Latency ổn định xem ở [`latency_report.md`](latency_report.md).

Source-to-producer lần này có giá trị nhỏ nhất 1,006 giây, so với 0,281 giây ở lần đo ngày 2026-11-07 trên cùng máy. Mức sàn dịch khoảng 0,7 giây giữa hai lần đo cho thấy có thể đồng hồ máy đã lệch, đúng với giới hạn đo đã ghi. Chưa kiểm chứng nguyên nhân.
