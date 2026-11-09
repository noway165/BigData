#!/bin/sh
# Kiểm tra ba metric analytics trên dữ liệu dựng sẵn. Không cần Kafka hay HDFS.
set -eu

exec /opt/spark/bin/spark-submit --master "local[1]" \
  "${APP_DIR:-/opt/app}/tests/spark_analytics_selfcheck.py"
