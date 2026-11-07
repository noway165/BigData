# Báo cáo latency

Số liệu gốc: [`latency_stable_run.json`](latency_stable_run.json).

## Kết quả

Đo trong lúc pipeline chạy ổn định, không có sự cố: 17.249 event, event time từ 17:14:37 đến 17:22:59 UTC ngày 2026-11-07. Không có mẫu âm ở cả ba chỉ số.

| Chỉ số | Công thức | p50 (giây) | p95 (giây) | Lớn nhất (giây) |
|---|---|---|---|---|
| Source-to-producer | `producer_received_at - event_time` | 0,303 | 0,407 | 1,315 |
| Producer-to-processing | `spark_processed_at - producer_received_at` | 14,858 | 29,149 | 43,193 |
| End-to-end observed | `spark_processed_at - event_time` | 15,178 | 29,457 | 43,490 |

## Cách hiểu

- Gần như toàn bộ end-to-end là thời gian chờ micro-batch. Với trigger 30 giây, event đến đều thì chờ từ 0 đến 30 giây: trung vị khoảng 15 giây, p95 khoảng 28,5 giây. Số đo được khớp với mức đó.
- Muốn giảm latency thì giảm `RAW_TRIGGER`. Đánh đổi là nhiều file nhỏ hơn trên HDFS. Con số này chưa được đo ở trigger khác.
- Giá trị lớn nhất 43 giây nhiều khả năng đến từ batch đầu tiên lúc Spark khởi động, nhưng chưa tách riêng để kiểm chứng.

## Cấu hình đo

| Mục | Giá trị |
|---|---|
| Trigger raw sink | 30 giây |
| Spark | `local[2]`, driver 768 MB |
| Dịch vụ | 1 Kafka broker, 1 NameNode, 1 DataNode, chạy Podman trên WSL2 |
| Máy | Windows 11, 7,36 GB RAM, 3,76 GB khả dụng cho container |

## Giới hạn đo

- **Đồng hồ Wikimedia và đồng hồ máy nhóm không được đồng bộ với nhau.** Source-to-producer và end-to-end so sánh hai đồng hồ này, nên có thể chứa một độ lệch cố định mà phép đo không tách được. Không có mẫu âm chỉ cho thấy độ lệch không đủ lớn để làm số âm, không chứng minh hai đồng hồ khớp.
- **Producer và Spark dùng chung đồng hồ** vì các container chạy trên cùng một kernel WSL2. Producer-to-processing không bị ảnh hưởng bởi độ lệch nói trên trong cấu hình này. Nếu tách sang nhiều máy thì phải đồng bộ NTP.
- **`spark_processed_at` là thời điểm micro-batch bắt đầu** (`current_timestamp()`), không gồm thời gian ghi file. Latency thực tế đến khi dữ liệu đọc được trên HDFS sẽ cao hơn.
- **`event_time` là `meta.dt`**, thời điểm Event Platform ghi nhận, muộn hơn `timestamp` của MediaWiki. Vì vậy source-to-producer không gồm phần độ trễ trước `meta.dt`, và không phải độ trễ mạng thuần túy.
- **End-to-end ở đây tính đến raw sink**, không tính đến Dashboard.
- **p50 và p95 dùng `percentile_approx`**, là giá trị xấp xỉ với độ chính xác 10.000.
- **Đây là một lần đo khoảng 8 phút trên live stream**, nên không đại diện cho mọi tải. Chưa lặp lại nhiều lần.

## Ảnh hưởng của sự cố

Cũng trên dữ liệu đó, sau khi thêm 2 sự cố (xem [`ingestion_verification.md`](ingestion_verification.md)), p95 tăng mạnh vì event replay được tính là nhận muộn. Vì vậy không dùng số này làm latency lúc ổn định.

| Chỉ số (34.061 event, có sự cố) | p50 (giây) | p95 (giây) | Lớn nhất (giây) |
|---|---|---|---|
| Source-to-producer | 0,316 | 53,158 | 102,909 |
| Producer-to-processing | 18,925 | 97,726 | 146,872 |
| End-to-end observed | 19,250 | 113,487 | 147,194 |
