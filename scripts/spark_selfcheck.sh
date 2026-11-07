#!/bin/sh
# Check the Spark transformations on sample events. Needs no Kafka or HDFS.
set -eu

exec /opt/spark/bin/spark-submit --master "local[1]" "${APP_DIR:-/opt/app}/tests/spark_selfcheck.py"
