"""Kiểm tra ba metric analytics trên dữ liệu dựng sẵn. Không cần Kafka hay HDFS.

Chạy trong container spark: see scripts/spark_analytics_selfcheck.sh
"""
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src" / "streaming"))

from pyspark.sql import SparkSession  # noqa: E402

from analytics_job import activity_spike, bot_ratio, edit_count, prepare, to_kafka_rows  # noqa: E402

spark = (SparkSession.builder.master("local[1]").appName("analytics_selfcheck")
         .config("spark.sql.session.timeZone", "UTC").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

WATERMARK = "1 minute"  # trên DataFrame batch, watermark bị bỏ qua; window vẫn tính như thật


def envelope(second, wiki, title, user, bot, event_type="edit"):
    event = {"wiki": wiki, "title": title, "type": event_type, "namespace": 0,
             "user": user, "timestamp": 1762532400 + second,
             "meta": {"dt": f"2026-11-07T16:20:{second:02d}.000Z", "id": f"id-{wiki}-{title}-{second}"}}
    if bot is not None:  # bot thiếu hẳn phải thành Unknown, không thành Human
        event["bot"] = bot
    return json.dumps({"event_id": event["meta"]["id"], "event_time": event["meta"]["dt"],
                       "producer_received_at": "2026-11-07T16:20:59.000Z",
                       "raw_value": json.dumps(event)})


rows = []
# 11 lượt sửa một bài trong 55 giây, 3 người: vượt ngưỡng 10 -> phải bị đánh dấu.
for i in range(11):
    rows.append(envelope(i * 5, "enwiki", "Hot", f"u{i % 3}", i < 2))
# 5 lượt sửa, thiếu trường bot -> Unknown, dưới ngưỡng.
for i in range(5):
    rows.append(envelope(i * 5, "enwiki", "Cold", "u9", None))
# wiki khác, toàn bot.
for i in range(3):
    rows.append(envelope(i * 5, "dewiki", "Andere", "bot1", True))
# Event không có event_time: không gán được vào window nào, phải bị loại.
rows.append(json.dumps({"event_id": None, "event_time": None,
                        "producer_received_at": "2026-11-07T16:20:59.000Z",
                        "raw_value": json.dumps({"wiki": "enwiki", "title": "NoTime"})}))

kafka_df = spark.createDataFrame([(v.encode(), 0, i) for i, v in enumerate(rows)],
                                 ["value", "partition", "offset"])
prepared = prepare(kafka_df).cache()
assert prepared.count() == len(rows) - 1, "dòng thiếu event_time phải bị loại"

counts = {r["wiki"]: r for r in edit_count(prepared, WATERMARK).collect()}
assert set(counts) == {"enwiki", "dewiki"}, counts
assert counts["enwiki"]["edit_count"] == 16, counts["enwiki"]
assert counts["enwiki"]["bot_edits"] == 2, counts["enwiki"]
assert counts["dewiki"]["edit_count"] == 3 and counts["dewiki"]["bot_edits"] == 3
assert str(counts["enwiki"]["window_start"]) == "2026-11-07 16:20:00"
assert str(counts["enwiki"]["window_end"]) == "2026-11-07 16:21:00"

ratio = bot_ratio(prepared, WATERMARK).collect()
assert len(ratio) == 1, ratio  # mọi event nằm trong cùng window 5 phút
r = ratio[0]
assert (r["total_events"], r["bot_count"], r["human_count"], r["unknown_count"]) == (19, 5, 9, 5)
assert abs(r["bot_share"] + r["human_share"] + r["unknown_share"] - 1.0) < 1e-6
assert str(r["window_start"]) == "2026-11-07 16:20:00"

spikes = activity_spike(prepared, WATERMARK).collect()
assert {s["title"] for s in spikes} == {"Hot"}, [s["title"] for s in spikes]
# Sliding window 2 phút trượt 30 giây: cùng một bài xuất hiện ở nhiều window chồng nhau.
assert len(spikes) >= 1 and all(s["edit_count"] > s["threshold_edits"] for s in spikes)
hot = max(spikes, key=lambda s: s["edit_count"])
assert hot["edit_count"] == 11 and hot["distinct_users"] == 3, hot
assert hot["bot_edits"] == 2 and hot["human_edits"] == 9, hot

payload = json.loads(to_kafka_rows(edit_count(prepared, WATERMARK), "edit_count").first()["value"])
assert payload["metric"] == "edit_count" and payload["counted_types"] == "all"
assert {"window_start", "window_end", "wiki", "edit_count", "emitted_at"} <= set(payload)
print(json.dumps(payload, indent=2))

spark.stop()
print("ANALYTICS SELFCHECK OK")
