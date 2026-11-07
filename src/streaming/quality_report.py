"""Batch report over the raw Parquet: data quality and the three latency metrics."""
import json
import os
import sys
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

REQUIRED_FIELDS = ["event_id", "event_time", "producer_received_at", "raw_value",
                   "wiki", "title", "type", "namespace", "user", "bot", "timestamp"]

# metric name: (later timestamp, earlier timestamp)
LATENCIES = {
    "source_to_producer": ("producer_received_at", "event_time"),
    "producer_to_processing": ("spark_processed_at", "producer_received_at"),
    "end_to_end": ("spark_processed_at", "event_time"),
}


def latency_stats(raw: DataFrame, later: str, earlier: str) -> dict:
    seconds = F.col(later).cast("double") - F.col(earlier).cast("double")
    return raw.where(seconds.isNotNull()).agg(
        F.count("*").alias("samples"),
        F.percentile_approx(seconds, 0.5, 10000).alias("p50_s"),
        F.percentile_approx(seconds, 0.95, 10000).alias("p95_s"),
        F.min(seconds).alias("min_s"),
        F.max(seconds).alias("max_s"),
        F.sum((seconds < 0).cast("int")).alias("negative_samples"),
    ).first().asDict()


def build_report(raw: DataFrame) -> dict:
    schema = raw.schema.simpleString()
    raw = raw.cache()
    total = raw.count()
    # Null ids are "missing", so they must not also count as duplicates.
    with_id = raw.where(F.col("event_id").isNotNull())
    duplicate_rows = with_id.count() - with_id.select("event_id").distinct().count()
    missing = raw.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in REQUIRED_FIELDS]).first()
    # Wikimedia numbers its own stream, so holes in meta.offset mean events that never reached us.
    source_offsets = (
        raw.select(F.get_json_object("raw_value", "$.meta.topic").alias("topic"),
                   F.get_json_object("raw_value", "$.meta.partition").alias("partition"),
                   F.get_json_object("raw_value", "$.meta.offset").cast("long").alias("offset"))
        .where(F.col("offset").isNotNull())
        .groupBy("topic", "partition")
        .agg(F.min("offset").alias("min"), F.max("offset").alias("max"),
             F.countDistinct("offset").alias("distinct"))
        .withColumn("missing", F.col("max") - F.col("min") + 1 - F.col("distinct"))
    )
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "total_rows": total,
        "event_time_range": [str(v) for v in raw.agg(F.min("event_time"), F.max("event_time")).first()],
        "parquet_schema": schema,
        "missing_by_field": missing.asDict() if total else {},
        "duplicate_event_id_rows": duplicate_rows,
        "source_offsets": [r.asDict() for r in source_offsets.collect()],
        "rows_by_type": {r["type"]: r["count"] for r in raw.groupBy("type").count().collect()},
        # Raw values only: the Bot/Human/Unknown metric belongs to the analytics job.
        "rows_by_bot_value": {str(r["bot"]): r["count"] for r in raw.groupBy("bot").count().collect()},
        "latency": {name: latency_stats(raw, *pair) for name, pair in LATENCIES.items()},
    }


def main():
    spark = (SparkSession.builder.appName("raw_quality_report")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    path = os.environ.get("RAW_OUTPUT_PATH", "hdfs://namenode:8020/data/raw/recent_changes")
    # Reading the sink directory (not its files) makes Spark honour _spark_metadata,
    # so partial files from a crashed batch are ignored.
    report = json.dumps(build_report(spark.read.parquet(path)), indent=2, default=str)
    print(report)
    if len(sys.argv) > 1:
        with open(sys.argv[1], "w", encoding="utf-8") as out:
            out.write(report + "\n")
    spark.stop()


if __name__ == "__main__":
    main()
