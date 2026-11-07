"""Streaming query: copy every Kafka event to Parquet on HDFS (Raw layer)."""
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (BooleanType, IntegerType, LongType, StringType, StructField,
                               StructType, TimestampType)

ENVELOPE_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("event_time", TimestampType()),
    StructField("producer_received_at", TimestampType()),
    StructField("raw_value", StringType()),
])

# Columns kept for inspection and reports; raw_value holds the whole event.
RAW_FIELDS_SCHEMA = StructType([
    StructField("wiki", StringType()),
    StructField("title", StringType()),
    StructField("type", StringType()),
    StructField("namespace", IntegerType()),
    StructField("user", StringType()),
    StructField("bot", BooleanType()),  # null means the source did not say; keep it null
    StructField("timestamp", LongType()),
])


def to_raw_rows(kafka_df: DataFrame) -> DataFrame:
    """Flatten Kafka rows into the raw table. A broken message stays as a row of nulls."""
    envelope = F.from_json(F.col("value").cast("string"), ENVELOPE_SCHEMA).alias("envelope")
    fields = F.from_json(F.col("envelope.raw_value"), RAW_FIELDS_SCHEMA).alias("fields")
    return (
        kafka_df.select(envelope, F.col("partition").alias("kafka_partition"),
                        F.col("offset").alias("kafka_offset"))
        .select("envelope.*", fields, "kafka_partition", "kafka_offset")
        .select("event_id", "event_time", "producer_received_at", "fields.*", "raw_value",
                "kafka_partition", "kafka_offset")
        .withColumn("spark_processed_at", F.current_timestamp())
        .withColumn("year", F.date_format("event_time", "yyyy"))
        .withColumn("month", F.date_format("event_time", "MM"))
        .withColumn("day", F.date_format("event_time", "dd"))
        .withColumn("hour", F.date_format("event_time", "HH"))
    )


def main():
    env = os.environ.get
    spark = (SparkSession.builder.appName("raw_to_hdfs")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", "3")
             # The HDFS client picks replication, and it defaults to 3 regardless of the cluster.
             .config("spark.hadoop.dfs.replication", env("HDFS_REPLICATION", "1"))
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    kafka_df = (spark.readStream.format("kafka")
                .option("kafka.bootstrap.servers", env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
                .option("subscribe", env("KAFKA_TOPIC", "wikipedia-recent-changes"))
                .option("startingOffsets", "earliest")  # used only without a checkpoint
                .option("maxOffsetsPerTrigger", env("RAW_MAX_OFFSETS_PER_TRIGGER", "50000"))
                .load())

    query = (to_raw_rows(kafka_df)
             .coalesce(1)  # one small file per batch per partition instead of three
             .writeStream.format("parquet")
             .queryName("raw_to_hdfs")
             .option("path", env("RAW_OUTPUT_PATH", "hdfs://namenode:8020/data/raw/recent_changes"))
             .option("checkpointLocation",
                     env("RAW_CHECKPOINT_PATH", "hdfs://namenode:8020/checkpoints/raw_to_hdfs"))
             .partitionBy("year", "month", "day", "hour")
             .trigger(processingTime=env("RAW_TRIGGER", "30 seconds"))
             .start())
    query.awaitTermination()


if __name__ == "__main__":
    main()
