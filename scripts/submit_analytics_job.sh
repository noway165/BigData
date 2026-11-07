#!/bin/sh
# Chạy các query analytics trong container Spark.
# Connector phải khớp phiên bản Spark và Scala của stack (docs/04_technology_stack.md).
set -eu

exec /opt/spark/bin/spark-submit \
  --master "${SPARK_MASTER:-local[2]}" \
  --driver-memory "${SPARK_DRIVER_MEMORY:-1g}" \
  --packages "${SPARK_KAFKA_PACKAGE:-org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6}" \
  --conf spark.jars.ivy="${SPARK_IVY_DIR:-/tmp/ivy}" \
  "${APP_DIR:-/opt/app}/src/streaming/analytics_job.py"
