#!/bin/sh
# Start the raw streaming query inside a Spark container.
# The connector must match the Spark and Scala versions the team's stack runs.
set -eu

exec /opt/spark/bin/spark-submit \
  --master "${SPARK_MASTER:-local[2]}" \
  --driver-memory "${SPARK_DRIVER_MEMORY:-768m}" \
  --packages "${SPARK_KAFKA_PACKAGE:-org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6}" \
  --conf spark.jars.ivy="${SPARK_IVY_DIR:-/tmp/ivy}" \
  "${APP_DIR:-/opt/app}/src/streaming/raw_to_hdfs.py"
