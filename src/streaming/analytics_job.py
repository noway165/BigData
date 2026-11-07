"""Streaming query: Kafka raw -> event-time window -> Kafka topic analytics.

Ba query độc lập, mỗi query một checkpoint riêng:
  edit_count      tumbling 1 phút, theo wiki
  bot_ratio       tumbling 5 phút, Bot/Human/Unknown
  activity_spike  sliding 2 phút / 30 giây, theo (wiki, title), lọc theo ngưỡng

Tất cả dùng outputMode append: có watermark thì mỗi window chỉ phát một lần khi
đóng, nên dashboard không phải lọc bản ghi lặp của cùng một window.
Cấu hình bằng biến môi trường, xem .env.example.
"""
import json
import os
import sys
import time

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from raw_to_hdfs import to_raw_rows  # noqa: E402

# Ngưỡng heuristic của spike. Không phải bằng chứng có edit war.
SPIKE_MIN_EDITS = int(os.environ.get("SPIKE_MIN_EDITS", "10"))
# 0 = tắt điều kiện thứ hai. Bật lên để thử nghiệm false positive do bot sửa hàng loạt.
SPIKE_MIN_DISTINCT_USERS = int(os.environ.get("SPIKE_MIN_DISTINCT_USERS", "0"))
# Rỗng = tính mọi type. Data contract mục 4: "edit,new" là lượt sửa thật.
EVENT_TYPES = [t for t in os.environ.get("ANALYTICS_EVENT_TYPES", "").split(",") if t]


def bot_category() -> Column:
    """bot=true là Bot, false là Human, null hoặc thiếu là Unknown. Không đổi null thành false."""
    return (F.when(F.col("bot") == F.lit(True), "Bot")
            .when(F.col("bot") == F.lit(False), "Human")
            .otherwise("Unknown"))


def prepare(kafka_df: DataFrame) -> DataFrame:
    """Dùng lại parse của raw sink, thêm bot_category và bỏ dòng không có event time.

    Dòng thiếu event_time không thể gán vào window nào, nên bị loại ở đây;
    quality_report.py vẫn đếm chúng trên raw Parquet.
    """
    rows = to_raw_rows(kafka_df).withColumn("bot_category", bot_category())
    rows = rows.where(F.col("event_time").isNotNull())
    if EVENT_TYPES:
        rows = rows.where(F.col("type").isin(EVENT_TYPES))
    return rows


def edit_count(rows: DataFrame, watermark: str) -> DataFrame:
    agg = (rows.where(F.col("wiki").isNotNull())
           .withWatermark("event_time", watermark)
           .groupBy(F.window("event_time", "1 minute"), F.col("wiki"))
           .agg(F.count(F.lit(1)).alias("edit_count"),
                F.sum((F.col("bot_category") == "Bot").cast("int")).alias("bot_edits")))
    return agg.select(F.col("window.start").alias("window_start"),
                      F.col("window.end").alias("window_end"),
                      "wiki", "edit_count", "bot_edits")


def bot_ratio(rows: DataFrame, watermark: str) -> DataFrame:
    """Mẫu số của tỷ lệ là tổng số event trong cùng window, kể cả Unknown."""
    agg = (rows.withWatermark("event_time", watermark)
           .groupBy(F.window("event_time", "5 minutes"))
           .agg(F.count(F.lit(1)).alias("total_events"),
                F.sum((F.col("bot_category") == "Bot").cast("int")).alias("bot_count"),
                F.sum((F.col("bot_category") == "Human").cast("int")).alias("human_count"),
                F.sum((F.col("bot_category") == "Unknown").cast("int")).alias("unknown_count")))
    share = lambda c: F.round(F.col(c) / F.col("total_events"), 4)  # noqa: E731
    return agg.select(F.col("window.start").alias("window_start"),
                      F.col("window.end").alias("window_end"),
                      "total_events", "bot_count", "human_count", "unknown_count",
                      share("bot_count").alias("bot_share"),
                      share("human_count").alias("human_share"),
                      share("unknown_count").alias("unknown_share"))


def activity_spike(rows: DataFrame, watermark: str) -> DataFrame:
    """Đột biến hoạt động chỉnh sửa, không khẳng định là tranh chấp.

    Giữ bot_edits và human_edits để người đọc biết đột biến do bot hay do người.
    """
    agg = (rows.where(F.col("wiki").isNotNull() & F.col("title").isNotNull())
           .withWatermark("event_time", watermark)
           .groupBy(F.window("event_time", "2 minutes", "30 seconds"),
                    F.col("wiki"), F.col("title"))
           .agg(F.count(F.lit(1)).alias("edit_count"),
                # countDistinct không dùng được trong streaming aggregation.
                F.approx_count_distinct("user").alias("distinct_users"),
                F.sum((F.col("bot_category") == "Bot").cast("int")).alias("bot_edits"),
                F.sum((F.col("bot_category") == "Human").cast("int")).alias("human_edits")))
    out = agg.select(F.col("window.start").alias("window_start"),
                     F.col("window.end").alias("window_end"),
                     "wiki", "title", "edit_count", "distinct_users", "bot_edits", "human_edits",
                     F.lit(SPIKE_MIN_EDITS).alias("threshold_edits"),
                     F.lit(SPIKE_MIN_DISTINCT_USERS).alias("threshold_distinct_users"))
    out = out.where(F.col("edit_count") > SPIKE_MIN_EDITS)
    if SPIKE_MIN_DISTINCT_USERS:
        out = out.where(F.col("distinct_users") > SPIKE_MIN_DISTINCT_USERS)
    return out


METRICS = {"edit_count": edit_count, "bot_ratio": bot_ratio, "activity_spike": activity_spike}


def to_kafka_rows(df: DataFrame, metric: str) -> DataFrame:
    """Hợp đồng output: key là tên metric, value là một JSON object mỗi dòng."""
    body = (df.withColumn("metric", F.lit(metric))
            .withColumn("counted_types", F.lit(",".join(EVENT_TYPES) if EVENT_TYPES else "all"))
            .withColumn("emitted_at", F.current_timestamp()))
    return body.select(F.lit(metric).alias("key"),
                       F.to_json(F.struct(*body.columns)).alias("value"))


def progress_summary(batches: list) -> dict:
    """Số liệu cho bảng so sánh watermark: late data, state, throughput, batch duration."""
    def pct(values, q):
        values = sorted(v for v in values if v is not None)
        if not values:
            return None
        return values[min(len(values) - 1, int(round(q * (len(values) - 1))))]

    state = [(b.get("stateOperators") or [{}])[0] for b in batches]
    # PySpark không trả key batchDuration; thời gian một batch nằm ở
    # durationMs.triggerExecution.
    trigger_ms = [(b.get("durationMs") or {}).get("triggerExecution") for b in batches]
    return {
        "batches": len(batches),
        "input_rows": sum(b.get("numInputRows") or 0 for b in batches),
        "rows_dropped_by_watermark": sum(s.get("numRowsDroppedByWatermark") or 0 for s in state),
        "max_state_rows": max([s.get("numRowsTotal") or 0 for s in state] or [0]),
        "max_state_bytes": max([s.get("memoryUsedBytes") or 0 for s in state] or [0]),
        "batch_ms_p50": pct(trigger_ms, 0.5),
        "batch_ms_p95": pct(trigger_ms, 0.95),
        "last_watermark": next((b.get("eventTime", {}).get("watermark")
                                for b in reversed(batches) if b.get("eventTime")), None),
    }


def run(queries: list, run_seconds: int, metrics_out: str, config: dict) -> None:
    """Chạy đến khi bị dừng, hoặc đủ run_seconds khi làm thí nghiệm."""
    seen, batches = set(), []
    started = time.time()
    try:
        while any(q.isActive for q in queries):
            for q in queries:
                for p in q.recentProgress:
                    p = p if isinstance(p, dict) else json.loads(p.json)
                    key = (str(q.id), p.get("batchId"))
                    if key not in seen:
                        seen.add(key)
                        batches.append(p)
            if run_seconds and time.time() - started >= run_seconds:
                print(f"[analytics] đủ RUN_SECONDS={run_seconds}, dừng", flush=True)
                break
            time.sleep(2)
    except KeyboardInterrupt:
        print("[analytics] nhận tín hiệu dừng", flush=True)
    finally:
        for q in queries:
            try:
                q.stop()
            except Exception as exc:  # dừng query không được che lỗi thật
                print(f"[analytics] stop lỗi: {exc}", file=sys.stderr)

    summary = progress_summary(batches)
    print(json.dumps({"config": config, "summary": summary}, indent=2), flush=True)
    if metrics_out:
        os.makedirs(os.path.dirname(metrics_out) or ".", exist_ok=True)
        with open(metrics_out, "w", encoding="utf-8") as fh:
            json.dump({"config": config, "summary": summary, "batches": batches}, fh,
                      indent=2, default=str)
        print(f"[analytics] ghi {metrics_out}", flush=True)


def main():
    env = os.environ.get
    watermark = env("WATERMARK", "1 minute")
    selected = [m.strip() for m in env("ANALYTICS_METRICS", ",".join(METRICS)).split(",") if m.strip()]
    unknown = [m for m in selected if m not in METRICS]
    if unknown:
        sys.exit(f"metric không tồn tại: {unknown}; hợp lệ: {list(METRICS)}")

    # Hậu tố checkpoint: chỉ dùng cho lần chạy thí nghiệm, để không ghi lên checkpoint demo.
    suffix = env("CHECKPOINT_SUFFIX", "")
    checkpoint_root = env("ANALYTICS_CHECKPOINT_ROOT", "hdfs://namenode:8020/checkpoints")

    spark = (SparkSession.builder.appName("analytics_job")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", env("ANALYTICS_SHUFFLE_PARTITIONS", "3"))
             .config("spark.hadoop.dfs.replication", env("HDFS_REPLICATION", "1"))
             # RocksDB giữ state ngoài heap JVM: cần khi thử watermark 5 phút trên máy ít RAM.
             .config("spark.sql.streaming.stateStore.providerClass",
                     "org.apache.spark.sql.execution.streaming.state."
                     "RocksDBStateStoreProvider")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    kafka_df = (spark.readStream.format("kafka")
                .option("kafka.bootstrap.servers", env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
                .option("subscribe", env("ANALYTICS_SOURCE_TOPIC",
                                         env("KAFKA_TOPIC", "wikipedia-recent-changes")))
                .option("startingOffsets", env("ANALYTICS_STARTING_OFFSETS", "latest"))
                # Retention xóa offset cũ thì query vẫn phải chạy tiếp, không được chết:
                # bài test recovery phải khởi động lại từ checkpoint.
                .option("failOnDataLoss", "false")
                .option("maxOffsetsPerTrigger", env("ANALYTICS_MAX_OFFSETS_PER_TRIGGER", "50000"))
                .load())

    rows = prepare(kafka_df)
    out_topic = env("KAFKA_ANALYTICS_TOPIC", "wikipedia-analytics")
    trigger = env("ANALYTICS_TRIGGER", "10 seconds")

    queries = []
    for metric in selected:
        name = f"analytics_{metric}{('_' + suffix) if suffix else ''}"
        checkpoint = f"{checkpoint_root}/{name}"
        print(f"[analytics] {metric} watermark={watermark} checkpoint={checkpoint}", flush=True)
        queries.append(
            to_kafka_rows(METRICS[metric](rows, watermark), metric)
            .writeStream.queryName(name)
            .format("kafka")
            .option("kafka.bootstrap.servers", env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
            .option("topic", out_topic)
            .option("checkpointLocation", checkpoint)
            .outputMode("append")
            .trigger(processingTime=trigger)
            .start())

    run(queries, int(env("RUN_SECONDS", "0")), env("METRICS_OUT", ""),
        {"watermark": watermark, "metrics": selected, "trigger": trigger,
         "run_seconds": int(env("RUN_SECONDS", "0")),
         "starting_offsets": env("ANALYTICS_STARTING_OFFSETS", "latest"),
         "source_topic": env("ANALYTICS_SOURCE_TOPIC", env("KAFKA_TOPIC", "wikipedia-recent-changes")),
         "max_offsets_per_trigger": env("ANALYTICS_MAX_OFFSETS_PER_TRIGGER", "50000"),
         "spike_min_edits": SPIKE_MIN_EDITS,
         "spike_min_distinct_users": SPIKE_MIN_DISTINCT_USERS,
         "counted_types": EVENT_TYPES or "all"})


if __name__ == "__main__":
    main()
