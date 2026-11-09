# Thí nghiệm và đánh giá

Tài liệu này chứa hai thí nghiệm của phần stream processing: so sánh watermark và phục hồi sau sự cố. Kết quả latency và chất lượng dữ liệu nằm ở [latency_report.md](../outputs/experiment_results/latency_report.md) và [ingestion_verification.md](../outputs/experiment_results/ingestion_verification.md). Kết quả analytics và ảnh Dashboard do phần Dashboard bổ sung.

Số liệu dưới đây là của một lần chạy thật ngày 2026-11-07, trên Windows 11 (16 GB RAM) với Podman 6.1.4 chạy trong WSL2 (7,6 GB khả dụng cho container): 1 broker Kafka, 1 NameNode, 1 DataNode, Spark `local[2]`, driver 1 GB. Một lần chạy trên một máy, không phải benchmark.

## 1. Thí nghiệm watermark

### Cách chạy

```bash
sh scripts/watermark_experiment.sh
```

Script lưu một mẫu từ topic raw, rồi replay **đúng mẫu đó** cho từng cấu hình watermark: 10 giây, 1 phút, 5 phút. Mỗi cấu hình được tạo lại topic replay và dùng checkpoint riêng, nên không có cấu hình nào chạy trên state của cấu hình trước.

| Tham số | Biến môi trường | Mặc định |
|---|---|---|
| Số message mẫu | `WM_SAMPLE_SIZE` | 20000 |
| Thời gian chạy mỗi cấu hình | `WM_RUN_SECONDS` | 180 giây |
| Metric đo | `WM_METRIC` | `edit_count` |
| Tốc độ nạp mỗi trigger | `WM_MAX_OFFSETS_PER_TRIGGER` | 2000 |
| Danh sách cấu hình | `WM_CONFIGS` | `10 seconds\|1 minute\|5 minutes` |

Vì sao phải replay: luồng live không lặp lại cùng phân bố event, nên chạy ba cấu hình ở ba thời điểm khác nhau sẽ so sánh ba tập dữ liệu khác nhau. Replay giữ nguyên tập dữ liệu, chỉ đổi watermark.

### Kết quả

`scripts/watermark_report.py` sinh sẵn bảng này từ các file JSON của lần chạy.

Thư mục kết quả: `outputs/experiment_results/watermark_20261107T190813Z/`.

| Chỉ số | 10 giây | 1 phút | 5 phút |
|---|---|---|---|
| Event đầu vào | 20000 | 20000 | 20000 |
| Dòng bị watermark loại (`numRowsDroppedByWatermark`) | 118 | 5 | 0 |
| State rows lớn nhất | 168 | 233 | 488 |
| State bytes lớn nhất | 117380 | 143548 | 176188 |
| Batch duration p50 (ms) | 1068 | 1288 | 1400 |
| Batch duration p95 (ms) | 2984 | 2359 | 2269 |
| Số batch | 12 | 12 | 12 |
| Watermark cuối | 18:48:15.770Z | 18:47:25.770Z | 18:43:25.770Z |

Metric `edit_count`, trigger 10 giây, `maxOffsetsPerTrigger` 2000, mỗi cấu hình chạy 180 giây trên cùng 20.000 event replay.

### Đọc kết quả thế nào

Lần chạy này cho thấy đúng đánh đổi lý thuyết:

- Watermark càng lớn, càng ít dòng bị loại: 118 dòng ở 10 giây, 5 dòng ở 1 phút, 0 dòng ở 5 phút. Trên 20.000 event thì 118 dòng là 0,59%.
- Watermark càng lớn, state càng lớn: 168 → 233 → 488 dòng state, 117 KB → 176 KB bộ nhớ. State tăng gần 3 lần trong khi chỉ cứu được 0,59% dữ liệu.
- Watermark cuối của từng cấu hình lùi lại đúng bằng khoảng watermark (18:48:15 / 18:47:25 / 18:43:25), nên window đóng chậm hơn và kết quả ra chậm hơn.
- Batch duration p50 tăng nhẹ theo state (1068 → 1288 → 1400 ms). p95 ngược lại, cao nhất ở cấu hình 10 giây (2984 ms): với 12 batch mỗi cấu hình, p95 chỉ là batch chậm nhất, nên không kết luận gì từ nó.

**Chọn 1 phút** cho hệ thống: chỉ loại 5 dòng trên 20.000, state bằng một nửa cấu hình 5 phút và window đóng sớm hơn 4 phút.

Giới hạn của chính thí nghiệm này:

- Dữ liệu replay mang event time cũ, nên phân bố độ trễ của nó là phân bố của lúc thu mẫu, không phải của lúc chạy thí nghiệm.
- `kafka-console-producer` chia partition khác nhau giữa các lần nạp: tổng vẫn đúng 20.000, nhưng phân bố là 6676/6630/6694, 6657/6675/6668 và 6677/6557/6766. Thứ tự event giữa các partition vì vậy không giống hệt nhau giữa ba lần chạy.
- 12 batch mỗi cấu hình là mẫu nhỏ. Chạy dài hơn hoặc lặp nhiều lần mới nói được về phân bố latency.

## 2. Thí nghiệm phục hồi sau sự cố

### Cách chạy

```bash
docker compose up -d && sh scripts/bootstrap.sh
docker compose --profile producer up -d
sh scripts/crash_recovery_test.sh
```

Kịch bản script thực hiện:

1. Chạy query analytics ổn định `RECOVERY_RUN1_SECONDS` giây (mặc định 240).
2. Ghi lại offset Kafka và nội dung thư mục checkpoint `offsets/`, `commits/`.
3. `kill -9` tiến trình driver Spark. Đây là chết đột ngột, không shutdown êm.
4. Chờ `RECOVERY_GAP_SECONDS` giây (mặc định 60) trong lúc producer vẫn gửi dữ liệu vào Kafka.
5. Khởi động lại **cùng query, cùng checkpoint**, chạy `RECOVERY_RUN2_SECONDS` giây.
6. Ghi lại checkpoint và offset sau khi chạy lại.

Mọi bằng chứng nằm trong `outputs/experiment_results/recovery_<timestamp>/`, kèm `README.md` liệt kê đúng những gì cần đối chiếu. Không xóa checkpoint giữa hai phase; muốn chạy một thí nghiệm mới thì đổi `RECOVERY_SUFFIX`.

### Kết quả

Thư mục bằng chứng: `outputs/experiment_results/recovery_20261107T185544Z/`. Query `analytics_edit_count_recovery`, watermark 1 phút, trigger 10 giây.

| Mốc | Thời điểm (UTC) | Offset cuối topic raw (p0/p1/p2) |
|---|---|---|
| Khởi động phase 1 | 18:55:45 | 13250 / 11954 / 12024 |
| Lúc `kill -9` | 19:00:04 | 16427 / 14676 / 14686 |
| Sau 60 giây query chết, producer vẫn gửi | 19:01:05 | 17155 / 15368 / 15346 |
| Khởi động lại cùng checkpoint | 19:01:07 | — |
| Kết thúc phase 2 | 19:06:08 | 21064 / 19016 / 18582 |

Điều đã chứng minh được:

| Bằng chứng | Giá trị quan sát |
|---|---|
| Batch đầu sau restart | `batchId` **25**, tiếp số của phase 1 (phase 1 dừng ở batch 24). Một query mới không có checkpoint sẽ bắt đầu lại từ batch 0. |
| `startOffset` của batch 25 | 16421 / 14672 / 14679 — nằm ngay dưới mức cuối topic lúc kill (16427 / 14676 / 14686) |
| Không nhảy tới cuối topic | Nếu bỏ qua checkpoint, với `startingOffsets=latest` query sẽ bắt đầu từ 17155 / 15368 / 15346 và bỏ qua 728 + 696 + 660 = 2084 event của khoảng chết |
| Xử lý bù phần tồn đọng | Batch 25 đọc 2419 dòng trong một lần, các batch sau trở lại mức bình thường (248 dòng) |
| Tổng phase 2 | 10.466 dòng trong 25 batch (25 → 49), không batch nào lỗi |
| Topic analytics tiếp tục tăng | 640 message lúc kill → 1041 message sau phase 2 |

`startOffset` thấp hơn mức cuối topic lúc kill 6/4/7 message trên từng partition: đó là phần batch cuối chưa commit xong khi bị `kill -9`, và Spark xử lý lại đúng phần đó. Đây là at-least-once ở chặng Kafka → Spark, không phải exactly-once.

### Guarantee rút ra được

Thí nghiệm này chứng minh **Kafka source offset recovery qua checkpoint của Spark**: không mất event nào của khoảng chết, và batch dở bị xử lý lại. Nó không chứng minh exactly-once cho toàn pipeline. Bảng guarantee theo từng chặng nằm ở [02_architecture.md](02_architecture.md) mục 6. Riêng Kafka sink của nhánh analytics là at-least-once: một batch chạy lại có thể phát lại message của cùng một window, nên Dashboard phải chịu được bản ghi lặp theo `(metric, window_start)`.

### Có thể làm thêm

Lần chạy trên chỉ là **một** lần. Chạy lại ít nhất một lần nữa (đổi `RECOVERY_SUFFIX` để dùng checkpoint mới) rồi mới nói về tính lặp lại. Sau đó có thể thử dừng Kafka broker trong lúc Spark đang chạy; đừng dàn trải nhiều loại sự cố trước khi kịch bản này chạy ổn định.

## 3. Giới hạn của thuật toán phát hiện đột biến

- Ngưỡng `edit_count > 10` trong 2 phút là heuristic, không phải kết quả hiệu chỉnh trên dữ liệu có nhãn.
- Một bot sửa hàng loạt tạo đột biến mà không có tranh chấp nào: vì vậy bản ghi output tách riêng `bot_edits` và `human_edits`. Bật `SPIKE_MIN_DISTINCT_USERS` để thử điều kiện thứ hai.
- Một sự kiện nóng ngoài đời cũng tạo nhiều lượt sửa từ nhiều người mà không phải tranh chấp. Nhiều lượt sửa không chứng minh được động cơ.
- `distinct_users` dùng `approx_count_distinct`, nên là số gần đúng; với ngưỡng nhỏ, sai số này có thể đổi kết quả lọc.
- Topic chứa mọi `type`, kể cả `categorize` và `log`. Mặc định đếm tất cả. Muốn chỉ tính lượt sửa thật thì đặt `ANALYTICS_EVENT_TYPES=edit,new`; trường `counted_types` trong output ghi lại lựa chọn đó.
