#!/bin/sh
# Tạo Kafka topic và thư mục HDFS sau khi `docker compose up -d`. Chạy lại được nhiều lần.
# Chạy từ thư mục gốc repo trên máy host: sh scripts/bootstrap.sh
set -eu

DC="${DOCKER:-docker}"
RAW_TOPIC="${KAFKA_TOPIC:-wikipedia-recent-changes}"
ANALYTICS_TOPIC="${KAFKA_ANALYTICS_TOPIC:-wikipedia-analytics}"
REPLAY_TOPIC="${KAFKA_REPLAY_TOPIC:-wikipedia-replay}"
PARTITIONS="${KAFKA_PARTITIONS:-3}"
RETENTION_MS="${KAFKA_RETENTION_MS:-86400000}"  # 24 giờ, theo data contract

kafka() { "$DC" exec -i kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 "$@"; }
hdfs() { "$DC" exec -i namenode hdfs dfs "$@"; }

create_topic() {
  topic="$1"; parts="$2"
  if kafka --list | grep -qx "$topic"; then
    echo "[skip] topic $topic đã có"
  else
    kafka --create --topic "$topic" --partitions "$parts" --replication-factor 1 \
      --config "retention.ms=$RETENTION_MS"
    echo "[ok  ] tạo topic $topic ($parts partition, retention ${RETENTION_MS}ms)"
  fi
}

create_topic "$RAW_TOPIC" "$PARTITIONS"
create_topic "$ANALYTICS_TOPIC" 1
# Topic replay: dùng cho thí nghiệm watermark, để so sánh trên cùng một tập dữ liệu.
create_topic "$REPLAY_TOPIC" "$PARTITIONS"

# Checkpoint phải nằm trên HDFS: bền vững, không mất khi container bị xóa.
for dir in /checkpoints /data/raw/recent_changes /experiments; do
  hdfs -mkdir -p "$dir"
done
hdfs -chmod -R 777 /checkpoints /data /experiments

echo "--- Kafka topic ---"
kafka --describe
echo "--- HDFS ---"
hdfs -ls -R /
echo "NameNode UI: http://localhost:${HDFS_UI_PORT:-9870}"
