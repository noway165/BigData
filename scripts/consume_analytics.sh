#!/bin/sh
# Đọc topic analytics để kiểm tra output trước khi Dashboard có sẵn.
# Chạy trên máy host: sh scripts/consume_analytics.sh [số message]
set -eu

DC="${DOCKER:-docker}"
MAX="${1:-10}"

exec "$DC" exec -i kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic "${KAFKA_ANALYTICS_TOPIC:-wikipedia-analytics}" \
  --from-beginning --property print.key=true --max-messages "$MAX"
