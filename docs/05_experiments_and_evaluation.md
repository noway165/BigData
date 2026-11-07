# Thí nghiệm và đánh giá

Tài liệu này chứa hai thí nghiệm của phần stream processing: so sánh watermark và phục hồi sau sự cố. Kết quả latency và chất lượng dữ liệu nằm ở [latency_report.md](../outputs/experiment_results/latency_report.md) và [ingestion_verification.md](../outputs/experiment_results/ingestion_verification.md). Kết quả analytics và ảnh Dashboard do phần Dashboard bổ sung.

> **Trạng thái:** script của cả hai thí nghiệm đã có và chạy được bằng một lệnh. Bảng số liệu bên dưới còn trống vì chưa chạy trên máy nào cả. Chạy script rồi dán số thật vào, không ước lượng.

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

| Chỉ số | 10 giây | 1 phút | 5 phút |
|---|---|---|---|
| Event đầu vào | | | |
| Dòng bị watermark loại (`numRowsDroppedByWatermark`) | | | |
| State rows lớn nhất | | | |
| State bytes lớn nhất | | | |
| Batch duration p50 (ms) | | | |
| Batch duration p95 (ms) | | | |
| Số batch | | | |

### Đọc kết quả thế nào

- Watermark nhỏ: state nhỏ, window đóng sớm, nhưng loại nhiều event đến trễ hơn.
- Watermark lớn: nhận thêm event trễ, nhưng giữ state lâu hơn nên tốn bộ nhớ, và kết quả của một window ra chậm hơn.
- Chọn cấu hình cuối theo đánh đổi giữa độ đầy đủ, state và độ trễ, không chọn vì một cấu hình trông nhanh hơn.
- Giới hạn: dữ liệu replay mang event time cũ, nên phân bố độ trễ của nó là phân bố của lúc thu mẫu, không phải của lúc chạy thí nghiệm.

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

| Mốc | Thời điểm (UTC) | Offset cuối topic raw | Batch cuối trong `commits/` | Số dòng đã xử lý |
|---|---|---|---|---|
| Trước khi kill | | | | |
| Lúc kill | | | | — |
| Sau khoảng chết (producer vẫn gửi) | | | — | — |
| Sau khi restart | | | | |

Điều phải chứng minh: batch đầu tiên sau khi restart bắt đầu từ đúng offset mà batch cuối trước khi chết đã commit, chứ không nhảy tới cuối topic. Offset này đọc được ở `sources[].startOffset` trong `phase2_progress.json` và ở file mới nhất trong `offsets/` của checkpoint.

### Guarantee rút ra được

Thí nghiệm này chứng minh **Kafka source offset recovery qua checkpoint của Spark**. Nó không chứng minh exactly-once cho toàn pipeline. Bảng guarantee theo từng chặng nằm ở [02_architecture.md](02_architecture.md) mục 6. Riêng Kafka sink của nhánh analytics là at-least-once: một batch chạy lại có thể phát lại message của cùng một window, nên Dashboard phải chịu được bản ghi lặp theo `(metric, window_start)`.

### Có thể làm thêm

Dừng Kafka broker trong lúc Spark đang chạy. Chỉ làm sau khi kịch bản Spark recovery đã chạy ổn định ít nhất hai lần, vì dàn trải nhiều loại sự cố trước đó chỉ làm kết quả khó diễn giải.

## 3. Giới hạn của thuật toán phát hiện đột biến

- Ngưỡng `edit_count > 10` trong 2 phút là heuristic, không phải kết quả hiệu chỉnh trên dữ liệu có nhãn.
- Một bot sửa hàng loạt tạo đột biến mà không có tranh chấp nào: vì vậy bản ghi output tách riêng `bot_edits` và `human_edits`. Bật `SPIKE_MIN_DISTINCT_USERS` để thử điều kiện thứ hai.
- Một sự kiện nóng ngoài đời cũng tạo nhiều lượt sửa từ nhiều người mà không phải tranh chấp. Nhiều lượt sửa không chứng minh được động cơ.
- `distinct_users` dùng `approx_count_distinct`, nên là số gần đúng; với ngưỡng nhỏ, sai số này có thể đổi kết quả lọc.
- Topic chứa mọi `type`, kể cả `categorize` và `log`. Mặc định đếm tất cả. Muốn chỉ tính lượt sửa thật thì đặt `ANALYTICS_EVENT_TYPES=edit,new`; trường `counted_types` trong output ghi lại lựa chọn đó.
