# Bằng chứng crash recovery — 20261107T185544Z

| Tham số | Giá trị |
|---|---|
| Query | `analytics_edit_count_recovery` |
| Checkpoint | `hdfs://namenode:8020/checkpoints/analytics_edit_count_recovery` |
| Watermark | 1 minute |
| Chạy trước khi kill | 240s |
| Thời gian query chết | 60s (producer vẫn gửi) |
| Chạy sau khi restart | 240s |

## Cần đối chiếu

1. `02_checkpoint_before_kill.txt` — batch cuối trước khi chết và offset Kafka của nó.
2. `phase2_spark.log` + `04_checkpoint_after_restart.txt` — batch đầu sau restart
   phải bắt đầu từ đúng offset mà batch cuối đã commit, không nhảy tới cuối topic.
3. `03_raw_offsets_after_gap.txt` so với `02_raw_offsets_at_kill.txt` — chứng minh
   dữ liệu vẫn vào Kafka trong lúc Spark chết.
4. `phase2_progress.json` — `sources[].startOffset` của batch đầu sau restart và
   số dòng đã xử lý; `04_analytics_offsets_after.txt` cho thấy topic analytics tiếp tục tăng.

Guarantee quan sát được ở đây là Kafka source offset recovery qua checkpoint của Spark,
không phải exactly-once toàn pipeline. Xem docs/05_experiments_and_evaluation.md.
