# So sánh watermark — watermark_20261107T190813Z

Cùng một mẫu dữ liệu được replay cho mọi cấu hình; mỗi cấu hình có checkpoint riêng.

| Chỉ số | 10seconds | 1minute | 5minutes |
|---|---|---|---|
| Watermark | 10 seconds | 1 minute | 5 minutes |
| Event đầu vào | 20000 | 20000 | 20000 |
| Dòng bị watermark loại | 118 | 5 | 0 |
| State rows lớn nhất | 168 | 233 | 488 |
| State bytes lớn nhất | 117380 | 143548 | 176188 |
| Batch duration p50 (ms) | 1068 | 1288 | 1400 |
| Batch duration p95 (ms) | 2984 | 2359 | 2269 |
| Số batch | 12 | 12 | 12 |
| Watermark cuối | 2026-11-07T18:48:15.770Z | 2026-11-07T18:47:25.770Z | 2026-11-07T18:43:25.770Z |

Metric: `['edit_count']`, trigger 10 seconds, maxOffsetsPerTrigger 2000, mỗi cấu hình chạy xem config giây.

Watermark không phải bộ lọc event cũ: nó quyết định khi nào Spark đóng window và bỏ state. Watermark lớn giữ state lâu hơn nên tốn bộ nhớ hơn, nhưng nhận được nhiều event đến trễ hơn.
