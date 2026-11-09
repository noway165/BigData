# Thiết kế: làm lại UI dashboard

Ngày 2026-11-09. Phạm vi: `src/dashboard/dashboard.py`, không đụng tới pipeline Spark.

## Vì sao

Dashboard hiện tại đủ để chứng minh pipeline chạy, nhưng khi chiếu lên màn hình lớn thì
nó nháy trắng mỗi 5 giây, nhảy về đầu trang, và hiển thị ít hơn hẳn lượng thông tin
đang có trong dữ liệu. Buổi bảo vệ cần một trang vừa dễ hiểu vừa dày thông tin.

## Mục tiêu

- Nhìn vào hiểu ngay hệ thống đang xử lý dữ liệu thật, theo thời gian thực.
- Dày thông tin nhưng không rối: mỗi khối trả lời đúng một câu hỏi.
- Đọc được từ xa khi chiếu.

## Không làm

- Không đổi `analytics_job.py`, không đổi hợp đồng JSON của topic analytics.
- Không thêm dependency Python, không thêm image, không thêm service.
- Không phụ thuộc mạng lúc chạy: trang không tải tài nguyên ngoài.
- Không lưu trữ lịch sử lâu dài. State nằm trong bộ nhớ, mất khi container restart.

## Ràng buộc

- Chạy trong image `wikipedia-sse-producer` (python:3.12-slim + confluent-kafka).
- Chỉ dùng thư viện chuẩn cho phần web.
- Phòng bảo vệ có thể không có mạng.

## Các hướng đã cân nhắc

| Hướng | Đánh đổi | Kết quả |
|---|---|---|
| Chỉ làm lại CSS, giữ meta refresh | Rẻ nhất, ít rủi ro nhất | Loại: vẫn nháy trắng và nhảy về đầu trang mỗi 5 giây |
| `/api/state` trả JSON + render phía client, SVG tự vẽ | Thêm khoảng 150 dòng JavaScript | **Chọn** |
| Như trên + Chart.js commit vào repo | Biểu đồ đẹp hơn, có tooltip sẵn | Loại: thêm ~200 KB mã third-party vào repo nhóm, phải giải thích khi bị hỏi |
| Chế độ trình chiếu tự xoay vòng toàn màn hình | Ấn tượng khi chạy nền | Loại: tốn nhiều thời gian nhất, dễ hỏng nhất khi làm sát ngày bảo vệ |

Người dùng giao lại quyết định; lựa chọn trên là của tôi.

## Nguồn dữ liệu

Trang đọc **hai** topic bằng hai consumer thread riêng:

| Topic | Offset bắt đầu | Dùng cho |
|---|---|---|
| `wikipedia-analytics` | `earliest`, group id mới mỗi lần khởi động | Toàn bộ phần số liệu tổng hợp. Đọc từ earliest nên trang có dữ liệu ngay. |
| `wikipedia-recent-changes` | `latest` | Chỉ cho dòng log sự kiện. Không đọc lịch sử: 24 giờ retention là hàng triệu event. |

Việc đọc topic raw là thay đổi kiến trúc duy nhất của thiết kế này, và nó chỉ phục vụ
khối log. Mọi khối khác vẫn chỉ phụ thuộc hợp đồng JSON của topic analytics.

## Nội dung trang

1. **Hàng KPI** — lượt sửa/phút của window mới nhất, tỷ lệ bot, số wiki đang hoạt động,
   số spike trong 10 phút gần nhất, độ trễ phát (`emitted_at` − `window_end`),
   tổng bản ghi analytics đã nhận.
2. **Biểu đồ theo thời gian** — lượt sửa mỗi phút, cột xếp chồng tách bot và người.
3. **Top wiki** — thanh xếp chồng bot/người kèm số tuyệt đối và phần trăm.
4. **Bot ratio theo thời gian** — đường xu hướng qua nhiều window 5 phút.
   Hiện tại trang chỉ hiện một window mới nhất rồi vứt phần còn lại.
5. **Dòng spike** — cường độ `edit_count / threshold_edits`, thanh mức, thời gian tương đối.
6. **Log sự kiện trực tiếp** — xem mục riêng bên dưới.
7. **Thanh trạng thái pipeline** — window mới nhất cách hiện tại bao lâu,
   trạng thái kết nối tới `/api/state`.

Khối 1 và 7 là bằng chứng trực tiếp rằng hệ thống đang chạy thật, không phải ảnh tĩnh.

## Log sự kiện trực tiếp

Mỗi dòng: thời gian `HH:MM:SS` theo event time, tên người sửa đã che, loại thay đổi,
tên bài, và mức thay đổi dung lượng.

Các quyết định của người dùng:

| Điểm | Quyết định | Ghi chú |
|---|---|---|
| Tên người dùng | **Che một phần**: `Printstream` thành `Pri***`, `171.224.3.9` thành `171.224.*.*` | Giữ đúng dòng "Không đưa tên tài khoản lên Dashboard" trong [03_data_contract.md](../../03_data_contract.md), vẫn thấy được là nhiều người khác nhau đang sửa |
| Wiki | **`enwiki` và `viwiki`** | Đo thật: 400 sự kiện trong 11 giây, commons 167 (toàn `File:...` của bot), wikidata 32 (toàn mã `Q...`), enwiki 28, viwiki 0. Lọc hai wiki này cho khoảng 2-3 dòng/giây, tên bài đọc được. Lựa chọn của tôi sau khi người dùng hỏi ý |
| Loại thay đổi | **Chỉnh sửa bài** (`type=edit`), **Thêm bài mới** (`type=new`) | Bỏ "Xoá bài": nó nằm trong `type=log` với `log_type=delete` và hiếm tới mức cả buổi demo có thể không có dòng nào |
| Bot | **Hiện, có nhãn Bot riêng** | Ăn khớp với biểu đồ bot_ratio phía trên, thầy đối chiếu được hai chỗ |

Quyết định của tôi, người dùng chưa nêu:

- **Không hiện `comment` của người sửa.** Đó là nội dung tự do của người lạ, chiếu lên
  máy chiếu thì không kiểm soát được nó viết gì.
- **Có hiện mức thay đổi dung lượng** (`length.new − length.old`) dạng `+412` / `−87`:
  một con số, không có nội dung tự do, cho thấy sửa nhiều hay ít.
- **Giữ 60 dòng gần nhất.** Dòng mới chèn trên đầu.
- **Lọc ngay trong consumer**, không lọc ở trình duyệt, để không phải đẩy
  34 sự kiện mỗi giây xuống trang rồi bỏ đi.

## Kỹ thuật

- Vẫn một file `src/dashboard/dashboard.py`, vẫn thư viện chuẩn.
- `/api/state` trả JSON; trang `fetch` mỗi 2 giây và cập nhật DOM tại chỗ, không reload.
- Biểu đồ là SVG viết tay, sinh từ dữ liệu trong JSON.
- `<noscript>` giữ nguyên bản render phía server như hiện nay, nên JavaScript hỏng
  thì trang vẫn có nội dung chứ không trắng.
- Sáng/tối theo `prefers-color-scheme`.
- State giữ thêm: lịch sử `bot_ratio` nhiều window, tách bot/người theo từng wiki,
  và deque log. Hai thứ đầu hiện đang bị vứt đi.
- Biến môi trường mới: `KAFKA_TOPIC` (topic raw), `DASHBOARD_LOG_WIKIS`,
  `DASHBOARD_LOG_LINES`. Thêm vào service `dashboard` trong `docker-compose.yml`
  và vào `.env.example`.

## Kiểm thử

Mở rộng `tests/dashboard_selfcheck.py`: state mới, che tên người dùng, lọc wiki và
loại thay đổi, hình dạng JSON của `/api/state`. Vẫn chỉ dùng thư viện chuẩn, vẫn
chạy được trên máy host không cài `confluent-kafka`.

## Việc còn lại cho người làm

- Chọn thang màu cụ thể và cỡ chữ.
- Quyết định bố cục lưới ở màn hình hẹp.

## Rủi ro

Thêm JavaScript là thêm chỗ có thể hỏng, và buổi bảo vệ là sáng mai.
`<noscript>` là lưới an toàn cho rủi ro đó.
