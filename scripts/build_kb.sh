#!/bin/sh
# Rebuild the knowledge base from raw Parquet. Run it inside the spark container.
set -eu

exec /opt/spark/bin/spark-submit \
  --master "local[2]" \
  --driver-memory "${SPARK_DRIVER_MEMORY:-768m}" \
  "${APP_DIR:-/opt/app}/src/streaming/build_knowledge_base.py"
