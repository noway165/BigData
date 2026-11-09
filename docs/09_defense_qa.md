# Trả lời 12 câu hỏi phản biện

Mỗi câu có: câu trả lời ngắn (nói trong 20–30 giây), phần giải thích, và bằng chứng của nhóm để chỉ ra nếu giảng viên hỏi tiếp. Số liệu lấy từ các lần chạy ngày 2026-11-07 trên một máy (Podman/WSL2, Spark `local[2]`).

---

### 1. Vì sao cần Kafka thay vì Producer gửi thẳng vào Spark?

**Ngắn:** Kafka tách nơi tạo dữ liệu khỏi nơi xử lý. Spark chết thì dữ liệu vẫn nằm trong Kafka và Spark đọc tiếp từ offset đã lưu. Gửi thẳng thì Spark chết là mất dữ liệu.

**Giải thích:**
- **Bộ đệm bền:** topic giữ 24 giờ. SSE không có cơ chế "đọc lại từ vị trí X" sau khi producer tắt hẳn, còn Kafka thì có.
- **Replay:** thí nghiệm watermark đọc lại cùng 20.000 event cho ba cấu hình. Không có Kafka thì không so sánh công bằng được.
- **Nhiều consumer độc lập:** raw sink và ba query analytics cùng đọc một topic với checkpoint riêng; Dashboard đọc một topic khác.
- **Hấp thụ chênh lệch tốc độ:** producer gửi đều ~30 event/giây, Spark đọc theo lô mỗi 10–30 giây.

**Bằng chứng:** bài test recovery — trong 60 giây Spark chết, 2.084 event vẫn vào Kafka và được Spark xử lý sau khi chạy lại.

---

### 2. Kafka partition key ảnh hưởng thế nào đến phân phối dữ liệu?

**Ngắn:** Key quyết định message vào partition nào (hash của key). Cùng key thì cùng partition, nên giữ được thứ tự. Nhóm dùng key `wiki:title`.

**Giải thích:**
- Mọi lượt sửa của một bài vào cùng một partition, nên thứ tự sửa của từng bài được giữ.
- Nếu chỉ dùng `wiki` làm key thì `commonswiki` và `wikidatawiki` — hai wiki đông nhất — dồn vào một hai partition, gây lệch tải (data skew).
- Key `wiki:title` có rất nhiều giá trị khác nhau nên tải chia khá đều. Kết quả thực tế: 12.048 / 11.081 / 10.932 message trên 3 partition.
- Đánh đổi: không giữ thứ tự giữa các bài khác nhau, nhưng analytics theo event time không cần thứ tự đó.

---

### 3. Event time khác processing time như thế nào?

**Ngắn:** Event time là lúc sự kiện xảy ra (`meta.dt`, nằm trong dữ liệu). Processing time là lúc Spark xử lý (đồng hồ máy). Nhóm tính window theo event time.

**Giải thích:**
- Nếu theo processing time, một lượt sửa lúc 10:00:59 đến trễ, được xử lý lúc 10:01:05, sẽ bị đếm vào phút 10:01 — sai.
- Theo event time, nó vẫn vào đúng window 10:00–10:01, miễn là chưa quá watermark.
- Khi Spark chạy lại sau sự cố và xử lý bù 2.419 dòng tồn đọng trong một batch, theo processing time tất cả sẽ dồn vào một phút; theo event time thì vẫn vào đúng phút của chúng.

**Đo được:** từ `meta.dt` đến lúc producer nhận: p50 0,30 giây, p95 0,41 giây.

---

### 4. Watermark giải quyết vấn đề gì? Vì sao watermark lớn làm tăng thời gian giữ state?

**Ngắn:** Watermark là mốc "không chờ event cũ hơn mức này nữa" = event time lớn nhất đã thấy trừ một khoảng trễ cho phép. Nhờ nó Spark biết khi nào đóng window, phát kết quả và xóa state. Watermark càng lớn thì window càng lâu mới đóng, nên state phải giữ lâu hơn.

**Giải thích:** không có watermark, Spark phải giữ state của mọi window mãi mãi vì không biết event trễ nào còn đến — bộ nhớ tăng vô hạn. Event đến sau khi window đã đóng thì bị bỏ.

**Bằng chứng (cùng 20.000 event replay):**

| Watermark | Dòng bị loại | State rows lớn nhất |
|---|---|---|
| 10 giây | 118 (0,59%) | 168 |
| 1 phút | 5 | 233 |
| 5 phút | 0 | 488 |

Chọn **1 phút**: chỉ mất 5 dòng, state bằng một nửa cấu hình 5 phút, kết quả ra sớm hơn 4 phút.

---

### 5. Checkpoint lưu những thông tin gì? Vì sao phải dùng checkpoint bền vững?

**Ngắn:** Checkpoint lưu (1) offset Kafka mỗi batch định đọc (`offsets/`), (2) batch nào đã xong (`commits/`), (3) state của aggregation (các window đang mở), và metadata của query. Phải để trên HDFS vì checkpoint trong filesystem tạm của container sẽ mất khi container bị xóa.

**Giải thích:** khi chạy lại, Spark so `offsets/` và `commits/`: batch nào có trong offsets mà chưa có trong commits thì chạy lại batch đó với đúng khoảng offset cũ. Mỗi query một thư mục checkpoint riêng (`/checkpoints/analytics_edit_count`, …) vì hai query dùng chung checkpoint sẽ ghi đè tiến độ của nhau.

**Bằng chứng:** sau `kill -9`, batch đầu tiên là batch **25** (nối tiếp batch 24), bắt đầu ở offset 16421/14672/14679 thay vì nhảy tới cuối topic 17155/15368/15346.

---

### 6. Exactly-Once khác At-Least-Once như thế nào? Nhóm chứng minh guarantee ở từng thành phần ra sao?

**Ngắn:** At-least-once: không mất, nhưng có thể trùng. Exactly-once: mỗi bản ghi có tác dụng đúng một lần. Nhóm không tuyên bố exactly-once cho toàn hệ thống mà nói theo từng chặng.

| Chặng | Guarantee | Bằng chứng |
|---|---|---|
| SSE → Producer | At-least-once (resume bằng `Last-Event-ID` có thể gửi lại) | Ngắt mạng 90 giây, log `resumed_from_last_id=True` |
| Producer → Kafka | Không mất, không trùng do retry (`acks=all`, idempotent producer) | Tắt Kafka 60 giây, `undelivered=0` |
| Kafka → Spark | Offset trong checkpoint; batch dở được chạy lại | Test recovery: batch 25 tiếp nối, xử lý lại 6/4/7 message chưa commit |
| Spark → Parquet | Không trùng file khi chạy lại (commit qua `_spark_metadata`) | 34.061 event ở producer = Kafka = Parquet, 0 dòng trùng `event_id` |
| Spark → Kafka analytics | At-least-once (Kafka sink không dùng transaction) | Dashboard khử trùng theo `(metric, window_start, …)` |

Kết luận: phần lưu raw đạt kết quả cuối không mất, không trùng trong các sự cố đã thử; nhánh analytics là at-least-once và được bù bằng khử trùng ở Dashboard.

---

### 7. Vì sao chọn Parquet thay vì chỉ lưu JSON?

**Ngắn:** Parquet lưu theo cột, có nén và có schema. Truy vấn chỉ đọc các cột cần, file nhỏ hơn, và Spark đọc nhanh hơn nhiều so với parse JSON từng dòng.

**Giải thích:**
- **Column pruning:** đếm theo `wiki` chỉ đọc cột `wiki`, không đọc cột `raw_value` dài.
- **Partition pruning:** thư mục `year=/month=/day=/hour=` cho phép truy vấn một giờ chỉ đọc đúng thư mục đó.
- **Có schema và kiểu:** `event_time` là timestamp, `bot` là boolean, không phải đoán kiểu mỗi lần đọc.
- Nhóm vẫn giữ JSON gốc trong cột `raw_value`, nên không mất thông tin nếu sau này cần trường khác.

**Đo được:** 34.061 event nằm trong 27 file, tổng 14,4 MB.

---

### 8. Sliding window khác tumbling window như thế nào?

**Ngắn:** Tumbling window nối tiếp nhau, không chồng lấn: mỗi event thuộc đúng một window. Sliding window có độ dài lớn hơn bước trượt nên chồng lấn: một event thuộc nhiều window.

**Trong đồ án:**
- Edit count: tumbling 1 phút (10:00–10:01, 10:01–10:02…). Cộng các window không bị đếm trùng.
- Bot ratio: tumbling 5 phút.
- Spike: sliding 2 phút, trượt 30 giây — mỗi event thuộc 4 window. Dùng sliding để không bỏ sót một đợt sửa dồn dập bị cắt đôi ở ranh giới window: 6 lượt cuối phút trước + 6 lượt đầu phút sau thì tumbling 1 phút không window nào vượt 10, nhưng sliding thì có.
- Hệ quả: một đợt spike hiện trong nhiều bản ghi; Dashboard gộp theo bài để không lặp.

---

### 9. Vì sao nhiều edit không đồng nghĩa với Edit War?

**Ngắn:** Edit war là hai bên hoàn tác qua lại nội dung của nhau. Nhóm chỉ đếm số lượt sửa, không đọc nội dung, nên chỉ gọi kết quả là "hoạt động chỉnh sửa tăng đột biến".

**Các trường hợp nhiều edit mà không có tranh chấp:**
- Bot sửa hàng loạt (rất phổ biến trên Wikidata, Commons) → output tách `bot_edits`/`human_edits`, Dashboard đánh dấu *Do bot*.
- Sự kiện nóng (bầu cử, thể thao, người nổi tiếng qua đời) → nhiều người cùng cập nhật, là hợp tác.
- Một người tự sửa liên tục nhiều lần nhỏ → điều kiện thử nghiệm `distinct_users > N`.

Muốn phát hiện edit war thật cần dữ liệu revert/diff — nằm ngoài phạm vi. Ngưỡng `> 10 lượt / 2 phút` là heuristic, chưa hiệu chỉnh trên dữ liệu có nhãn.

---

### 10. Nếu producer mất kết nối SSE thì hệ thống xử lý và ghi nhận khoảng gián đoạn như thế nào?

**Ngắn:** Producer tự kết nối lại và gửi `Last-Event-ID`, nên Wikimedia phát tiếp từ event cuối đã nhận. Khoảng gián đoạn được ghi trong log và kiểm tra lại bằng báo cáo chất lượng dữ liệu.

**Giải thích:**
- Phát hiện mất kết nối bằng lỗi kết nối hoặc read timeout; kết nối lại với backoff tăng dần.
- Log ghi `SSE disconnected` và `SSE gap closed gap_s=...`; bộ đếm `reconnects`.
- Kafka tắt thì producer giữ message trong hàng đợi (tối đa 20.000) và gửi khi Kafka sống lại.
- Kiểm chứng độc lập: `quality_report.py` đọc offset nguồn của Wikimedia trong raw Parquet và đếm lỗ hổng (`missing`).

**Bằng chứng:** ngắt mạng 90 giây → tự nối lại, `resumed_from_last_id=True`. **Giới hạn:** nếu producer tắt hẳn rồi khởi động lại, `Last-Event-ID` chỉ nằm trong RAM nên mất — một lần thử ghi nhận `missing = 6.187`.

---

### 11. Dashboard có làm chậm Spark không? Nhóm đã tách processing và visualization ra sao?

**Ngắn:** Không. Spark ghi kết quả đã tổng hợp vào topic `wikipedia-analytics`; Dashboard là một consumer Kafka riêng, chạy ở tiến trình/container riêng. Hai bên chỉ gặp nhau qua Kafka.

**Giải thích:**
- Dashboard không đọc raw stream, không tính lại aggregation — chỉ đọc vài trăm bản ghi đã tổng hợp mỗi phút.
- Dashboard dùng `group.id` riêng, không commit offset, nên không ảnh hưởng tiến độ của consumer nào khác.
- Dashboard chết thì Spark vẫn chạy; Spark chết thì Dashboard vẫn hiển thị số cũ và cập nhật tiếp khi Spark chạy lại.
- Vì Kafka sink là at-least-once, Dashboard khử bản ghi lặp và hiển thị số bản đã khử.

---

### 12. Nếu tăng số lượng partition hoặc tốc độ event, nút thắt cổ chai nằm ở đâu?

**Ngắn:** Với cấu hình hiện tại, nút thắt là Spark `local[2]` (2 core trên một máy), không phải Kafka. Tăng partition chỉ có ích khi có thêm core/executor để đọc song song.

**Giải thích:**
- Số task đọc Kafka bằng số partition (3). Với 2 core, partition thứ 3 phải chờ; tăng lên 12 partition mà vẫn 2 core thì không nhanh hơn.
- Aggregation có shuffle (`spark.sql.shuffle.partitions = 3`) và state store; watermark lớn làm state tăng (168 → 488 dòng khi tăng 10 giây → 5 phút).
- `maxOffsetsPerTrigger` (50.000) giới hạn lượng đọc mỗi batch; batch p50 ~1,1–1,4 giây trong khi trigger 10 giây, nên hiện còn dư năng lực nhiều.
- Producer Python một luồng và một broker Kafka cũng là giới hạn nếu tải lên rất cao.
- Một máy 7,6 GB RAM chạy cả Kafka, HDFS, Spark nên RAM cũng là giới hạn.

**Lưu ý trung thực:** nhóm chưa chạy benchmark tải cao. Luồng thật chỉ ~30 event/giây; các nhận định trên dựa trên cấu hình và số đo batch duration, không phải đo throughput tối đa.
