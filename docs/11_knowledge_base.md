# Knowledge base về hoạt động chỉnh sửa Wikipedia

Knowledge base này biến dữ liệu thô trên HDFS thành kho kiến thức có cấu trúc về các wiki và bài viết, tra cứu được theo tên bài.

Nó chứa kiến thức về **hoạt động chỉnh sửa**: bài nào được sửa, bao nhiêu lần, do bot hay người, có đột biến không. Nó **không** chứa nội dung bài viết hay tên người dùng.

## Các bảng

Job `src/streaming/build_knowledge_base.py` đọc raw Parquet, chỉ giữ `edit` và `new`, khử bản ghi trùng, rồi ghi vào `hdfs://namenode:8020/data/kb/`.

| Bảng | Mỗi dòng là | Trường chính |
|---|---|---|
| `wiki_profile` | Một wiki | `edits`, `bot_share`, `articles`, `editors`, `first_seen`, `last_seen` |
| `article_profile` | Một bài viết | `wiki`, `title`, `edits`, `bot_edits`, `human_edits`, `editors`, `spike_bursts`, `first_seen`, `last_seen` |
| `spike_history` | Một đợt đột biến | `wiki`, `title`, `started_at`, `ended_at`, `peak_edits`, `peak_editors` |

Quy tắc đột biến và phân loại Bot/Human/Unknown dùng lại đúng hàm của `analytics_job.py`, nên knowledge base không lệch định nghĩa với Dashboard. Các window trượt chồng nhau của một bài được gộp thành một đợt.

## Cách dùng

Stack phải đang có HDFS và Spark. Dựng hoặc cập nhật knowledge base (mỗi lần chạy dựng lại toàn bộ từ raw):

```bash
podman exec spark sh /opt/app/scripts/build_kb.sh
```

Mở trang tra cứu trên máy host, từ thư mục gốc repo:

```bash
streamlit run src/kb/app.py --server.port 8502
```

Trang ở `http://localhost:8502`, gồm ba tab: tra cứu bài viết, danh sách wiki, các đợt đột biến.

Knowledge base **không tự cập nhật**. Muốn có dữ liệu mới thì chạy lại lệnh dựng.

## Kết quả đã dựng

Dựng ngày 2026-11-10 từ khoảng 33 phút dữ liệu thật:

| Chỉ số | Giá trị |
|---|---|
| Lượt sửa | 39.643 |
| Bài viết | 35.091 |
| Wiki | 167 |
| Đợt đột biến | 7, trên 6 bài |
| Thời gian dựng | Khoảng 40 giây |

## Kiểm chứng

- Job tự dừng nếu tổng lượt sửa của bảng wiki hoặc bảng bài viết khác tổng event.
- `tests/spark_kb_selfcheck.py` kiểm tra khử trùng, loại event không phải lượt sửa và gộp đợt đột biến.
- `tests/test_kb_view.py` kiểm tra logic tìm kiếm.
- Đối chiếu với topic analytics: cả 5 bài analytics báo đột biến đều có trong knowledge base. Bài thứ 6 (`Q142097538`) đột biến lúc 06:24, trước khi job analytics bật, nên chỉ knowledge base có.

## Giới hạn

- Chỉ có các bài được sửa trong lúc hệ thống thu dữ liệu.
- Không trả lời được câu hỏi về nội dung bài viết.
- Field `bot` lấy từ Wikimedia: tài khoản dùng công cụ tự động mà không có cờ bot vẫn tính là Human.
- `editors` đếm theo tên tài khoản hoặc IP, không phân biệt được một người dùng nhiều tài khoản.
