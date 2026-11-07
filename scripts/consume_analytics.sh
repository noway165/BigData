#!/bin/sh
# Đọc topic analytics để kiểm tra output trước khi Dashboard có sẵn.
# Chạy trên máy host: sh scripts/consume_analytics.sh [số message]
set -eu

# Git Bash trên Windows tự đổi /opt/... thành C:/Program Files/Git/opt/..., nên
# đường dẫn trong container bị sai. Hai biến này tắt việc chuyển đổi đó.
MSYS_NO_PATHCONV=1
MSYS2_ARG_CONV_EXCL='*'
export MSYS_NO_PATHCONV MSYS2_ARG_CONV_EXCL

DC="${DOCKER:-docker}"
MAX="${1:-10}"

exec "$DC" exec -i kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic "${KAFKA_ANALYTICS_TOPIC:-wikipedia-analytics}" \
  --from-beginning --property print.key=true --max-messages "$MAX"
