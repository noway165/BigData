#!/bin/sh
# So sánh watermark 10 giây / 1 phút / 5 phút trên CÙNG một tập dữ liệu.
#
# Live stream không lặp lại cùng phân bố event, nên script lưu một mẫu từ topic raw,
# rồi replay đúng mẫu đó cho từng cấu hình watermark. Khác cách này thì bảng so sánh
# không công bằng và phải ghi rõ giới hạn trong báo cáo.
#
# Chạy trên máy host:  sh scripts/watermark_experiment.sh
set -eu

DC="${DOCKER:-docker}"
PY="${PYTHON:-python}"
SAMPLE_SIZE="${WM_SAMPLE_SIZE:-20000}"
RUN_SECONDS="${WM_RUN_SECONDS:-180}"
METRIC="${WM_METRIC:-edit_count}"
REPLAY_TOPIC="${KAFKA_REPLAY_TOPIC:-wikipedia-replay}"
PARTITIONS="${KAFKA_PARTITIONS:-3}"
CONFIGS="${WM_CONFIGS:-10 seconds|1 minute|5 minutes}"

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT_REL="outputs/experiment_results/watermark_$STAMP"
OUT_IN_CONTAINER="/opt/app/$OUT_REL"
mkdir -p "$OUT_REL"
SAMPLE_IN_CONTAINER="/opt/app/$OUT_REL/sample.jsonl"

log() { echo "[$(date -u +%H:%M:%S)] $*" | tee -a "$OUT_REL/timeline.log"; }

kcli() { "$DC" exec -i kafka /opt/kafka/bin/"$@"; }

log "lưu mẫu $SAMPLE_SIZE message từ ${KAFKA_TOPIC:-wikipedia-recent-changes}"
kcli kafka-console-consumer.sh --bootstrap-server kafka:9092 \
  --topic "${KAFKA_TOPIC:-wikipedia-recent-changes}" \
  --from-beginning --max-messages "$SAMPLE_SIZE" > "$OUT_REL/sample.jsonl"
wc -l "$OUT_REL/sample.jsonl" | tee -a "$OUT_REL/timeline.log"

OLD_IFS="$IFS"
IFS='|'
for watermark in $CONFIGS; do
  IFS="$OLD_IFS"
  slug="$(echo "$watermark" | tr -d ' ' | tr '[:upper:]' '[:lower:]')"
  log "cấu hình watermark=$watermark"

  # Tạo lại topic replay để mọi lần chạy đọc đúng cùng một dãy offset.
  kcli kafka-topics.sh --bootstrap-server kafka:9092 --delete --topic "$REPLAY_TOPIC" 2>/dev/null || true
  sleep 5
  kcli kafka-topics.sh --bootstrap-server kafka:9092 --create --topic "$REPLAY_TOPIC" \
    --partitions "$PARTITIONS" --replication-factor 1 >/dev/null
  "$DC" exec -i kafka /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server kafka:9092 --topic "$REPLAY_TOPIC" < "$OUT_REL/sample.jsonl"
  kcli kafka-get-offsets.sh --bootstrap-server kafka:9092 --topic "$REPLAY_TOPIC" \
    > "$OUT_REL/${slug}_replay_offsets.txt"

  # Checkpoint riêng cho từng cấu hình: không bao giờ chạy lại trên state của cấu hình khác.
  "$DC" exec -i \
    -e ANALYTICS_METRICS="$METRIC" \
    -e WATERMARK="$watermark" \
    -e CHECKPOINT_SUFFIX="wm_${slug}_${STAMP}" \
    -e ANALYTICS_SOURCE_TOPIC="$REPLAY_TOPIC" \
    -e ANALYTICS_STARTING_OFFSETS="earliest" \
    -e ANALYTICS_MAX_OFFSETS_PER_TRIGGER="${WM_MAX_OFFSETS_PER_TRIGGER:-2000}" \
    -e RUN_SECONDS="$RUN_SECONDS" \
    -e METRICS_OUT="$OUT_IN_CONTAINER/${slug}.json" \
    spark sh -lc "sh /opt/app/scripts/submit_analytics_job.sh > $OUT_IN_CONTAINER/${slug}_spark.log 2>&1" \
    || log "cấu hình $watermark kết thúc với lỗi, xem ${slug}_spark.log"
  IFS='|'
done
IFS="$OLD_IFS"

"$PY" scripts/watermark_report.py "$OUT_REL" | tee "$OUT_REL/README.md"
log "xong. Kết quả: $OUT_REL"
