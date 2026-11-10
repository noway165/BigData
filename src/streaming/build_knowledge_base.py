"""Batch job: turn raw Parquet into a searchable knowledge base about Wikipedia editing.

Tables are written to HDFS, and exported as JSONL for the search page in src/kb/app.py.
Run it with scripts/build_kb.sh. Each run rebuilds everything from the raw data.
"""
import json
import os
import sys
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from analytics_job import activity_spike, bot_category  # noqa: E402

EDIT_TYPES = ("edit", "new")


def clean_events(raw: DataFrame) -> DataFrame:
    """Real edits only, one row per event: a redelivered event must not count twice."""
    return (raw.where(F.col("event_time").isNotNull() & F.col("wiki").isNotNull()
                      & F.col("title").isNotNull() & F.col("type").isin(*EDIT_TYPES))
            .withColumn("dedupe_key", F.coalesce("event_id", "raw_value"))
            .dropDuplicates(["dedupe_key"])
            .withColumn("bot_category", bot_category()))


def edit_stats(events: DataFrame, keys: list, *extra) -> DataFrame:
    count_of = lambda name: F.sum((F.col("bot_category") == name).cast("int"))  # noqa: E731
    return events.groupBy(*keys).agg(
        F.count(F.lit(1)).alias("edits"),
        count_of("Bot").alias("bot_edits"),
        count_of("Human").alias("human_edits"),
        count_of("Unknown").alias("unknown_edits"),
        F.countDistinct("user").alias("editors"),
        F.min("event_time").alias("first_seen"),
        F.max("event_time").alias("last_seen"),
        *extra,
    )


def wiki_profile(events: DataFrame) -> DataFrame:
    return (edit_stats(events, ["wiki"], F.countDistinct("title").alias("articles"))
            .withColumn("bot_share", F.round(F.col("bot_edits") / F.col("edits"), 4)))


def spike_history(events: DataFrame) -> DataFrame:
    """One row per burst: overlapping and adjacent sliding windows of an article are merged."""
    by_article = Window.partitionBy("wiki", "title").orderBy("window_start")
    starts_new_burst = F.col("window_start") > F.lag("window_end").over(by_article)
    return (activity_spike(events, "1 minute")
            .withColumn("new_burst", F.coalesce(starts_new_burst, F.lit(True)).cast("int"))
            .withColumn("burst", F.sum("new_burst").over(by_article))
            .groupBy("wiki", "title", "burst")
            .agg(F.min("window_start").alias("started_at"),
                 F.max("window_end").alias("ended_at"),
                 F.max("edit_count").alias("peak_edits"),
                 F.max("distinct_users").alias("peak_editors"),
                 F.max("bot_edits").alias("peak_bot_edits"),
                 F.max("human_edits").alias("peak_human_edits"),
                 F.count(F.lit(1)).alias("windows_flagged"))
            .drop("burst"))


def article_profile(events: DataFrame, spikes: DataFrame) -> DataFrame:
    bursts = spikes.groupBy("wiki", "title").agg(F.count(F.lit(1)).alias("spike_bursts"))
    return (edit_stats(events, ["wiki", "title"])
            .join(bursts, ["wiki", "title"], "left")
            .fillna({"spike_bursts": 0}))


def export_jsonl(df: DataFrame, path: str) -> int:
    """Written beside the target and swapped in, so the page never reads a half-written file."""
    rows = 0
    with open(path + ".tmp", "w", encoding="utf-8", newline="\n") as out:
        for row in df.toLocalIterator():
            out.write(json.dumps(row.asDict(), default=str, ensure_ascii=False) + "\n")
            rows += 1
    os.replace(path + ".tmp", path)
    return rows


def main():
    env = os.environ.get
    spark = (SparkSession.builder.appName("build_knowledge_base")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", "4")
             .config("spark.hadoop.dfs.replication", env("HDFS_REPLICATION", "1"))
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    raw_path = env("RAW_OUTPUT_PATH", "hdfs://namenode:8020/data/raw/recent_changes")
    kb_root = env("KB_HDFS_PATH", "hdfs://namenode:8020/data/kb")
    export_dir = env("KB_EXPORT_DIR", "/opt/app/outputs/kb")
    os.makedirs(export_dir, exist_ok=True)

    events = clean_events(spark.read.parquet(raw_path)).cache()
    total = events.count()
    spikes = spike_history(events).orderBy(F.desc("peak_edits")).cache()
    tables = {
        "wiki_profile": wiki_profile(events).orderBy(F.desc("edits")),
        "article_profile": article_profile(events, spikes).orderBy(F.desc("edits")),
        "spike_history": spikes,
    }
    # Every event belongs to exactly one wiki and one article, so both totals must match.
    for name in ("wiki_profile", "article_profile"):
        counted = tables[name].agg(F.sum("edits")).first()[0] or 0
        if counted != total:
            raise SystemExit(f"{name} counts {counted} edits, expected {total}")

    counts = {}
    for name, table in tables.items():
        table.write.mode("overwrite").parquet(f"{kb_root}/{name}")
        counts[name] = export_jsonl(table, os.path.join(export_dir, f"{name}.jsonl"))

    first, last = events.agg(F.min("event_time"), F.max("event_time")).first()
    meta = {"built_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "edits": total, "event_time_range": [str(first), str(last)], "rows": counts,
            "hdfs_path": kb_root}
    with open(os.path.join(export_dir, "meta.json"), "w", encoding="utf-8") as out:
        json.dump(meta, out, ensure_ascii=False, indent=2)
    print(json.dumps(meta, ensure_ascii=False), flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
