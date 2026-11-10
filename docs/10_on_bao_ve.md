# Tài liệu ôn bảo vệ

Đọc file này trước giờ bảo vệ. 12 câu phản biện đã có câu trả lời ở [09_defense_qa.md](09_defense_qa.md), file này không chép lại.

Mọi số dưới đây đều đo trên máy của nhóm, một lần chạy, không phải benchmark. Khi nói số, nói kèm điều kiện.

## 1. Nói trong 30 giây

> Nhóm xây một knowledge base về hoạt động chỉnh sửa Wikipedia. Producer Python đọc luồng SSE của Wikimedia và đẩy vào Kafka. Spark Structured Streaming lưu dữ liệu thô dạng Parquet xuống HDFS, từ đó một job Spark dựng các bảng kiến thức về wiki, bài viết và các đợt đột biến để tra cứu. Song song, nhánh analytics tính số liệu theo event time cho Dashboard. Nhóm còn đo latency, so sánh watermark, và cố tình làm hỏng hệ thống để chứng minh dữ liệu không mất.

## 2. Dữ liệu đi qua hệ thống

| Chặng | Việc xảy ra | Vì sao làm vậy |
|---|---|---|
| Wikimedia SSE | Phát mỗi lượt sửa thành một event JSON | Nguồn công khai, có sẵn thời gian thực |
| Producer | Thêm `producer_received_at`, bỏ event canary, gửi vào Kafka, tự kết nối lại | Ghi được thời điểm nhận để đo latency, không mất event khi mạng đứt |
| Kafka `wikipedia-recent-changes` | Giữ dữ liệu 24 giờ, 3 partition, key `wiki:title` | Tách producer khỏi Spark, Spark chết thì dữ liệu vẫn còn |
| Spark raw (`raw_to_hdfs`) | Ghi Parquet mỗi 30 giây vào `year=/month=/day=/hour=` | Lưu lại toàn bộ để kiểm tra và đo |
| Spark analytics (3 query) | Tính edit count, bot ratio, activity spike theo event time | Kết quả đúng theo lúc sự kiện xảy ra |
| Kafka `wikipedia-analytics` | Giữ kết quả đã tổng hợp | Dashboard và Spark không phụ thuộc nhau |
| Dashboard `:8501`, Live feed `:8080` | Hiển thị số liệu; Live feed thêm log từng lượt sửa | Dashboard chỉ đọc, không tính lại |
| Job `build_knowledge_base`, trang `:8502` | Từ raw Parquet dựng bảng wiki, bài viết, đợt đột biến để tra cứu | Biến luồng sự kiện thành kiến thức có cấu trúc |

## 3. Phần việc từng người

| Người | Phụ trách |
|---|---|
| **Lâm** | Producer, data contract, raw Parquet trên HDFS, đo 3 loại latency, kiểm tra chất lượng dữ liệu, hướng dẫn cài đặt |
| **Giang** | Docker Compose, Spark core và analytics job, watermark, crash recovery, Live feed, `demo.bat` |
| **Khoa** | Định nghĩa metric, Dashboard Streamlit, câu trả lời phản biện, slide |

## 4. Số cần thuộc

### Ingestion và lưu trữ (Lâm)

| Số | Giá trị | Nói kèm |
|---|---|---|
| Đối chiếu qua sự cố | Producer = Kafka = Parquet = **34.061**, trùng **0** | Có ngắt mạng producer 90 giây và tắt Kafka 60 giây |
| Đối chiếu trên stack chung | **13.778** ở cả ba nơi, trùng 0, hở offset 0 | Lần này không tạo sự cố |
| Phân bố 3 partition | 12.048 / 11.081 / 10.932 | Key `wiki:title` chia tải khá đều |
| Parquet | 27 file, 14,4 MB, 34.061 event | Replication 1 vì chỉ có 1 DataNode |
| Khảo sát schema | 1.882 event trong 60 giây; 620 là `categorize`; `id` thiếu ở 22 event | Vì vậy dùng `meta.id` làm khóa, không dùng `id` |

### Latency (17.249 event, chạy ổn định, trigger 30 giây)

| Chỉ số | p50 | p95 |
|---|---|---|
| Source-to-producer | 0,303 giây | 0,407 giây |
| Producer-to-processing | 14,858 giây | 29,149 giây |
| End-to-end | 15,178 giây | 29,457 giây |

Điều phải giải thích được: end-to-end khoảng 15 giây là do **chờ micro-batch**. Trigger 30 giây thì event chờ từ 0 đến 30 giây, trung vị khoảng 15 giây, p95 khoảng 28,5 giây. Số đo được khớp với mức đó.

### Watermark (cùng 20.000 event replay)

| Watermark | Dòng bị loại | State rows lớn nhất |
|---|---|---|
| 10 giây | 118 | 168 |
| **1 phút (chọn)** | **5** | **233** |
| 5 phút | 0 | 488 |

### Crash recovery (`kill -9` Spark, query edit count)

- Spark chết 60 giây, trong lúc đó **2.084 event** vẫn vào Kafka.
- Batch đầu sau khi chạy lại là **25**, nối tiếp batch 24, không bắt đầu lại từ 0.
- Batch 25 xử lý bù **2.419 dòng** một lần.
- Không nhảy tới cuối topic, nên không bỏ sót event của khoảng chết.

## 5. Khái niệm nói trong một câu

| Khái niệm | Nói thế này |
|---|---|
| Event time | Lúc sự kiện xảy ra, nằm trong dữ liệu (`meta.dt`). |
| Processing time | Lúc Spark xử lý, lấy từ đồng hồ máy. |
| Tumbling window | Các khung nối tiếp, không chồng lấn, mỗi event thuộc đúng một khung. |
| Sliding window | Các khung chồng lấn, một event thuộc nhiều khung. |
| Watermark | Mốc để Spark ngừng chờ event trễ, đóng window và xóa state. |
| State | Phần Spark phải nhớ cho các window chưa đóng. |
| Checkpoint | Nơi lưu offset, batch đã xong và state, để chạy lại đúng chỗ. |
| Offset | Số thứ tự của message trong một partition Kafka. |
| Partition key | Giá trị quyết định message vào partition nào; cùng key thì cùng partition. |
| Micro-batch | Spark xử lý theo từng lô nhỏ mỗi vài giây, không xử lý từng event. |
| At-least-once | Không mất, nhưng có thể trùng. |
| Exactly-once | Mỗi bản ghi có tác dụng đúng một lần. Nhóm **không** tuyên bố điều này cho toàn hệ thống. |
| Idempotent producer | Kafka tự bỏ bản gửi lại do retry trong cùng một phiên. |
| Parquet | Lưu theo cột, có schema và nén, đọc vài cột không phải quét toàn bộ. |
| Canary event | Event giám sát của Wikimedia, không phải lượt sửa thật, nên producer bỏ. |

## 6. Trình tự demo

Trọng tâm là knowledge base. Pipeline và recovery là phần chứng minh nó được xây trên dữ liệu thật.

**Chuẩn bị trước giờ trình bày, làm đúng thứ tự:**

1. Nhấp đúp `demo.bat`, để chạy **ít nhất 30 phút** cho có dữ liệu.
2. Tạm dừng analytics để đủ RAM dựng knowledge base. Chạy cả hai cùng lúc từng làm treo máy.

```bash
podman exec spark pkill -9 -f "[a]nalytics_job"
```

3. Dựng knowledge base, mất khoảng 1 phút:

```bash
podman exec spark sh /opt/app/scripts/build_kb.sh
```

4. Bật lại analytics:

```bash
podman exec -d -e ANALYTICS_EVENT_TYPES=edit,new spark sh /opt/app/scripts/submit_analytics_job.sh
```

5. Mở trang tra cứu từ thư mục gốc repo:

```bash
.venv\Scripts\python -m streamlit run src\kb\app.py --server.port 8502
```

6. Chép sẵn tên một bài đứng đầu bảng để tìm khi demo, và thử bấm **Mở** một lần.

**Khi trình bày:**

| Bước | Chỉ vào | Nói |
|---|---|---|
| 1 | Trang `:8502`, bốn số trên cùng | "Đây là knowledge base về hoạt động chỉnh sửa Wikipedia, dựng từ dữ liệu thật mà hệ thống đã thu." |
| 2 | Live feed `:8080` | "Nguồn tri thức là luồng sửa thật của Wikipedia, đang chạy ngay bây giờ." |
| 3 | NameNode `:9870` | "Spark lưu dữ liệu thô theo `year/month/day/hour`, rồi một job Spark dựng ba bảng kiến thức ở `/data/kb`." |
| 4 | Tab Tra cứu, gõ tên bài | "Em tra một bài: biết nó được sửa bao nhiêu lần, do bot hay người, bao nhiêu người sửa, có đột biến không." |
| 5 | Bấm **Mở** | "Đây là lịch sử sửa của đúng bài đó trên Wikipedia, kiến thức trong kho khớp với nguồn thật." |
| 6 | Tab Wiki | "Kho cũng tổng hợp theo từng wiki: wiki nào hoạt động nhiều, tỷ lệ bot bao nhiêu." |
| 7 | Tab Đột biến | "Mỗi dòng là một đợt, đã gộp các window trượt chồng nhau. Đây là đột biến hoạt động, không khẳng định có edit war." |
| 8 | Dashboard `:8501`, kill Spark (lệnh dưới) | "Phần realtime: em giết Spark ngay bây giờ. Kafka vẫn nhận, số 'Kafka giữ chờ Spark' tăng dần." |
| 9 | Bật lại Spark | "Spark đọc tiếp từ checkpoint, biểu đồ không có lỗ hổng. Knowledge base được xây trên chính nguồn dữ liệu bền vững này." |
| 10 | Bảng số liệu | "Qua sự cố, số event ở producer, Kafka và Parquet đều khớp ở 34.061." |

Lệnh kill:

```bash
podman exec spark pkill -9 -f "[a]nalytics_job"
```

Chờ khoảng 90 giây cho banner đỏ hiện, rồi bật lại:

```bash
podman exec -d -e ANALYTICS_EVENT_TYPES=edit,new spark sh /opt/app/scripts/submit_analytics_job.sh
```

Nếu có sự cố, xem bảng cuối [08_demo_script.md](08_demo_script.md).

## 7. Câu hỏi ngoài danh sách 12 câu

**Knowledge base này chứa kiến thức gì?**
Kiến thức về hoạt động chỉnh sửa: wiki nào, bài nào, sửa bao nhiêu, bot hay người, có đột biến không. Không chứa nội dung bài viết. Chi tiết ở [11_knowledge_base.md](11_knowledge_base.md).

**Sao không tự cập nhật theo thời gian thực?**
Dữ liệu thô vào HDFS liên tục, nhưng các bảng kiến thức được dựng lại theo lô bằng một job Spark. Nhóm chọn lô để tổng hợp trên toàn bộ dữ liệu cho đơn giản và đúng.

**Vì sao không gọi thẳng API Wikipedia?**
EventStreams đẩy sự kiện ngay khi có. Gọi API phải hỏi liên tục, dễ bỏ sót và dễ bị giới hạn tần suất.

**Vì sao dùng `meta.dt` làm event time, không dùng `timestamp`?**
`meta.dt` có mili giây, còn `timestamp` chỉ tới giây. Đổi lại, `meta.dt` là lúc Event Platform ghi nhận, muộn hơn `timestamp` khoảng 1,24 giây (p50, mẫu 448 event).

**Làm sao biết dữ liệu là thật?**
Nhóm so 45 mã revision của bài `Q141010864` trong Parquet với API Wikimedia, cả 45 đều tồn tại. Đây là kiểm tra thủ công trên một bài.

**Bot được xác định thế nào? Có chính xác không?**
Dùng field `bot` của Wikimedia. Không hoàn toàn chính xác: tài khoản dùng công cụ chạy theo lô mà không có cờ bot vẫn bị xếp vào Human. Ví dụ thật: bài `Q141010864` có 45 lượt sửa trong 2 phút từ một tài khoản chạy QuickStatements, nhưng được tính là Human.

**Vì sao số Unknown bằng 0?**
Trong dữ liệu đã thấy, field `bot` luôn có. Nhóm vẫn giữ nhóm Unknown để không tự coi giá trị thiếu là Human.

**Vì sao chỉ đếm `edit` và `new`?**
Khoảng một phần ba event là `categorize`, không phải sửa nội dung. Tính cả vào thì số lượt sửa bị thổi lên và bảng đột biến toàn trang `Category:`.

**Vì sao giữ nguyên JSON gốc trong `raw_value`?**
Spark chỉ tách vài cột cần dùng. Giữ bản gốc thì sau này cần trường khác không phải thu lại.

**Kafka replication factor 1 có an toàn không?**
Không, vì chỉ có một broker. Đây là môi trường lab, nhóm không tuyên bố high availability.

**Sao Spark chết khi tắt Kafka?**
Spark không kết nối được broker trong khoảng một phút thì query dừng. Phải khởi động lại, và query đọc tiếp từ checkpoint. Đã thử: số event vẫn khớp, không trùng.

**Sao số liệu đầu tiên của Dashboard thấp hơn Parquet?**
Analytics bắt đầu từ `latest`, còn raw đọc từ `earliest`, nên phút đầu analytics chỉ thấy một phần. Các phút sau khớp tuyệt đối trong lần đối chiếu (7 phút liên tiếp, và 9 phút liên tiếp ở lần có kill Spark).

**Sao source-to-producer có lúc 0,3 giây, có lúc 1 giây?**
Giá trị nhỏ nhất đo được đổi từ 0,281 giây sang 1,006 giây giữa hai ngày. Nghi là đồng hồ máy lệch so với Wikimedia, nhưng chưa kiểm chứng.

**Producer tắt hẳn thì sao?**
Mất phần event phát ra trong lúc tắt, vì `Last-Event-ID` chỉ nằm trong RAM. Báo cáo chất lượng phát hiện được qua lỗ hổng offset. Một lần thử ghi nhận `missing` = 6.187.

## 8. Không được nói

| Đừng nói | Nói thế này |
|---|---|
| "Real-time tức thì" | "Kết quả có sau khi window đóng, trễ khoảng 2 phút" |
| "Exactly-once toàn hệ thống" | "Guarantee khác nhau theo từng chặng" |
| "Phát hiện được edit war" | "Phát hiện đột biến hoạt động chỉnh sửa" |
| "Xử lý được X nghìn event mỗi giây" | "Luồng thật chỉ vài chục event mỗi giây, nhóm chưa chạy benchmark tải cao" |
| "Hệ thống phân tán nhiều node" | "Chạy một máy, Spark `local[2]`, một broker, một DataNode" |
| "Latency chỉ 0,3 giây" | "0,3 giây là chặng đầu; end-to-end khoảng 15 giây do trigger 30 giây" |
| "Không bao giờ mất dữ liệu" | "Không mất trong các sự cố đã thử" |
| "Knowledge base biết nội dung Wikipedia" | "Knowledge base về hoạt động chỉnh sửa, không chứa nội dung bài" |

## 9. Giới hạn nên chủ động nói

- Mỗi thí nghiệm chạy một lần trên một máy, chưa lặp lại nhiều lần.
- Ngưỡng hơn 10 lượt sửa trong 2 phút là heuristic, chưa kiểm chứng trên dữ liệu có nhãn.
- Đồng hồ máy nhóm và đồng hồ Wikimedia không được đồng bộ, nên source-to-producer và end-to-end có thể chứa một độ lệch cố định.
- `spark_processed_at` là lúc batch bắt đầu, chưa gồm thời gian ghi file.
- Chưa chạy trên Docker thật, toàn bộ chạy bằng Podman.
- Dashboard hiển thị giờ UTC, Live feed hiển thị giờ Việt Nam.
