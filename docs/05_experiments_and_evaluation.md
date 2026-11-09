# Thí nghiệm và đánh giá

Tài liệu này chứa hai thí nghiệm của phần stream processing: so sánh watermark và phục hồi sau sự cố. Kết quả latency và chất lượng dữ liệu nằm ở [latency_report.md](../outputs/experiment_results/latency_report.md) và [ingestion_verification.md](../outputs/experiment_results/ingestion_verification.md). Kết quả analytics và Dashboard nằm ở mục 4.

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

## 4. Analytics và Dashboard

### Cách chạy

```bash
docker exec -d spark sh /opt/app/scripts/submit_analytics_job.sh
docker compose --profile dashboard up -d      # http://localhost:8501
```

Hoặc chạy Dashboard trên máy host (Kafka mở cổng 9094):

```bash
pip install -r src/dashboard/requirements.txt
streamlit run src/dashboard/app.py
```

Dự phòng khi Kafka không chạy được lúc demo: `ANALYTICS_SOURCE_FILE=<file.jsonl> streamlit run src/dashboard/app.py`. Lưu trước output thật bằng `sh scripts/capture_analytics.sh 5000` (ghi `outputs/experiment_results/analytics_capture.jsonl`), rồi chạy `ANALYTICS_SOURCE_FILE=outputs/experiment_results/analytics_capture.jsonl streamlit run src/dashboard/app.py`. File `tests/sample_events/analytics_sample.jsonl` là dữ liệu **tổng hợp** chỉ để test, không dùng làm số liệu báo cáo.

### Dashboard hiển thị gì

| Khu vực | Nguồn | Ghi chú |
|---|---|---|
| Event trong window 1 phút mới nhất, so với phút trước | `edit_count`, cộng mọi wiki | Theo event time |
| Tỷ lệ Human, số bài có spike, số wiki hoạt động | ba metric | |
| Số event mỗi phút, tách Bot / Human + Unknown | `edit_count` | Biểu đồ vùng xếp chồng |
| Top wiki trong N phút gần nhất | `edit_count` | N chỉnh được ở sidebar |
| Donut Bot / Human / Unknown và đường tỷ lệ theo thời gian | `bot_ratio` | Mẫu số là tổng event của cùng window 5 phút |
| Bảng cảnh báo hoạt động tăng đột biến | `activity_spike` | Gộp window trượt chồng nhau theo bài; cột *Do bot* |
| Độ trễ phát kết quả, message hỏng, bản ghi lặp đã khử | mọi metric | Trong khung mở rộng cuối trang |

### Thiết kế đáng chú ý

- **Tách processing và visualization.** Dashboard chỉ đọc topic `wikipedia-analytics`, không đọc raw stream và không tính lại aggregation. Dashboard chậm hay chết không ảnh hưởng Spark; Spark chết thì Dashboard vẫn hiện số cũ và tiếp tục khi Spark chạy lại.
- **Chịu bản ghi lặp.** Kafka sink là at-least-once (mục 2). Dashboard khử trùng theo khóa tự nhiên — `(window_start, wiki)` cho edit count, `window_start` cho bot ratio, `(window_start, wiki, title)` cho spike — và giữ bản có `emitted_at` mới nhất. Số bản đã khử hiển thị trên trang, nên sau bài test recovery có thể chỉ ra trực tiếp.
- **Không mất lịch sử khi mở lại.** Consumer dùng `group.id` ngẫu nhiên, đọc từ đầu topic và không commit offset.
- **Message hỏng không làm sập trang.** JSON lỗi hoặc metric lạ được đếm và hiển thị.

### Kiểm thử

`tests/test_dashboard_view.py` (10 test, chạy bằng `python -m pytest tests -q`) kiểm tra: đếm message hỏng, khử trùng giữ bản mới nhất, tổng window và chênh lệch, Top wiki chỉ tính N window gần nhất, mẫu số Bot/Human/Unknown gồm Unknown, gộp spike theo bài và cờ *Do bot*, độ trễ phát kết quả, và trường hợp chưa có dữ liệu.

Hạn chế của phần kiểm thử: test chạy trên logic xử lý bảng (`analytics_view.py`), không chạy giao diện Streamlit. Giao diện được kiểm tra bằng cách chạy với pipeline thật và chụp ảnh.

### Kết quả

| Bằng chứng | Trạng thái |
|---|---|
| Ảnh Dashboard chạy trên pipeline thật | `outputs/experiment_results/dashboard_live.png` — **cần chụp khi chạy thật** |
| Ảnh bảng spike có ít nhất một dòng | `outputs/experiment_results/dashboard_spike.png` — **cần chụp khi chạy thật** |
| Số bản ghi lặp đã khử sau bài test recovery | ghi lại từ khung "Độ trễ phát kết quả" |

Khi ghi số liệu vào báo cáo, ghi kèm thời điểm chạy, `counted_types` và watermark.
