"""Spark checks that need a JVM. Runs in the spark container: see scripts/spark_selfcheck.sh."""
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src" / "streaming"))

from pyspark.sql import SparkSession  # noqa: E402

from quality_report import build_report  # noqa: E402
from raw_to_hdfs import to_raw_rows  # noqa: E402

spark = (SparkSession.builder.master("local[1]").appName("selfcheck")
         .config("spark.sql.session.timeZone", "UTC").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

samples = (ROOT / "tests/sample_events/recentchange_sample.jsonl").read_text("utf-8").splitlines()
envelopes = [json.dumps({"event_id": json.loads(s)["meta"]["id"],
                         "event_time": json.loads(s)["meta"]["dt"],
                         "producer_received_at": "2026-11-07T16:20:30.000Z",
                         "raw_value": s}) for s in samples]
envelopes += [
    envelopes[0],  # duplicate
    json.dumps({"event_id": None, "event_time": "2026-11-07T16:20:00.000Z",
                "producer_received_at": "2026-11-07T16:19:59.000Z",  # clock skew
                "raw_value": json.dumps({"wiki": "enwiki", "title": "T", "type": "edit"})}),  # no bot
    "not json",
]
kafka_df = spark.createDataFrame([(v.encode(), 0, i) for i, v in enumerate(envelopes)],
                                 ["value", "partition", "offset"])
rows = to_raw_rows(kafka_df)

first = rows.where(rows.kafka_offset == 0).first()
expected = json.loads(samples[0])
assert first.event_id == expected["meta"]["id"]
assert first.wiki == expected["wiki"] and first.title == expected["title"]
assert first.bot == expected["bot"] and first.namespace == expected["namespace"]
assert (first.year, first.month, first.day, first.hour) == ("2026", "11", "07", "16")
assert first.raw_value == samples[0]
assert first.spark_processed_at is not None

report = build_report(rows)
print(json.dumps(report, indent=2, default=str))
assert report["total_rows"] == len(envelopes)
assert report["duplicate_event_id_rows"] == 1
assert report["rows_by_bot_value"]["None"] == 2  # missing bot + bad JSON
assert sum(report["rows_by_bot_value"].values()) == len(envelopes)
assert report["missing_by_field"]["event_id"] == 2  # explicit null + bad JSON
source = report["latency"]["source_to_producer"]
assert source["negative_samples"] == 1 and source["samples"] == len(envelopes) - 1
# The bad-JSON row has no producer time, so it must not be a latency sample.
assert report["latency"]["producer_to_processing"]["samples"] == len(envelopes) - 1
assert report["latency"]["end_to_end"]["samples"] == len(envelopes) - 1
offsets = sorted({json.loads(s)["meta"]["offset"] for s in samples})
assert len(report["source_offsets"]) == 1, report["source_offsets"]
assert report["source_offsets"][0]["missing"] == offsets[-1] - offsets[0] + 1 - len(offsets)

spark.stop()
print("SELFCHECK OK")
