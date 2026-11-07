#!/bin/sh
# Write the raw data quality and latency report. Run it inside a Spark container.
set -eu

APP_DIR="${APP_DIR:-/opt/app}"
exec /opt/spark/bin/spark-submit \
  --master "local[1]" \
  --driver-memory "${SPARK_DRIVER_MEMORY:-512m}" \
  "$APP_DIR/src/streaming/quality_report.py" \
  "$APP_DIR/outputs/experiment_results/raw_quality_report.json"
