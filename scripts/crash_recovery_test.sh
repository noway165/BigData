#!/bin/sh
# Bài test Fault Tolerance: kill Spark query rồi khởi động lại với CHECKPOINT CŨ.
# Bằng chứng được ghi vào outputs/experiment_results/recovery_<timestamp>/.
#
# Điều kiện: compose đang chạy, topic đã tạo, producer đang gửi dữ liệu liên tục.
#   docker compose up -d && sh scripts/bootstrap.sh
#   docker compose --profile producer up -d
# Chạy trên máy host từ thư mục gốc repo:
#   sh scripts/crash_recovery_test.sh
set -eu

DC="${DOCKER:-docker}"
METRIC="${RECOVERY_METRIC:-edit_count}"
SUFFIX="${RECOVERY_SUFFIX:-recovery}"
QUERY="analytics_${METRIC}_${SUFFIX}"
CHECKPOINT="${ANALYTICS_CHECKPOINT_ROOT:-hdfs://namenode:8020/checkpoints}/$QUERY"
CHECKPOINT_PATH="/checkpoints/$QUERY"
RAW_TOPIC="${KAFKA_TOPIC:-wikipedia-recent-changes}"
ANALYTICS_TOPIC="${KAFKA_ANALYTICS_TOPIC:-wikipedia-analytics}"
RUN1="${RECOVERY_RUN1_SECONDS:-240}"   # chạy ổn định trước khi kill
GAP="${RECOVERY_GAP_SECONDS:-60}"      # thời gian Spark chết, producer vẫn gửi
RUN2="${RECOVERY_RUN2_SECONDS:-240}"   # chạy lại sau recovery

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT_REL="outputs/experiment_results/recovery_$STAMP"
OUT_IN_CONTAINER="/opt/app/$OUT_REL"
mkdir -p "$OUT_REL"

log() { echo "[$(date -u +%H:%M:%S)] $*" | tee -a "$OUT_REL/timeline.log"; }

kafka_end_offsets() {
  "$DC" exec -i kafka /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server kafka:9092 --topic "$1" 2>/dev/null
}

checkpoint_state() {
  # offsets/ = batch đã lên kế hoạch, commits/ = batch đã ghi xong.
  "$DC" exec -i namenode sh -lc "
    hdfs dfs -ls $CHECKPOINT_PATH/offsets 2>/dev/null | tail -5
    echo '--- nội dung file offsets mới nhất ---'
    last=\$(hdfs dfs -ls -C $CHECKPOINT_PATH/offsets 2>/dev/null | grep -E '/[0-9]+\$' | sort -t/ -k5 -n | tail -1)
    [ -n \"\$last\" ] && hdfs dfs -cat \"\$last\"
    echo '--- commits ---'
    hdfs dfs -ls -C $CHECKPOINT_PATH/commits 2>/dev/null | tail -3
  " 2>/dev/null
}

start_job() {
  phase="$1"; seconds="$2"
  "$DC" exec -d \
    -e ANALYTICS_METRICS="$METRIC" \
    -e CHECKPOINT_SUFFIX="$SUFFIX" \
    -e WATERMARK="${WATERMARK:-1 minute}" \
    -e RUN_SECONDS="$seconds" \
    -e METRICS_OUT="$OUT_IN_CONTAINER/${phase}_progress.json" \
    -e ANALYTICS_STARTING_OFFSETS="latest" \
    spark sh -lc "sh /opt/app/scripts/submit_analytics_job.sh > $OUT_IN_CONTAINER/${phase}_spark.log 2>&1"
}

job_pid() { "$DC" exec -i spark sh -lc "pgrep -f analytics_job.py || true" | tr -d '\r'; }

wait_for_job_exit() {
  i=0
  while [ -n "$(job_pid)" ] && [ "$i" -lt 120 ]; do sleep 5; i=$((i + 1)); done
}

log "metric=$METRIC checkpoint=$CHECKPOINT run1=${RUN1}s gap=${GAP}s run2=${RUN2}s"
if [ -z "$("$DC" ps --format '{{.Names}}' | grep -x producer || true)" ]; then
  log "CẢNH BÁO: không thấy container producer. Bài test cần dữ liệu vào Kafka trong lúc Spark chết."
fi

# ---------- Phase 1: chạy ổn định ----------
log "PHASE 1: khởi động query, chạy ${RUN1}s"
kafka_end_offsets "$RAW_TOPIC" > "$OUT_REL/01_raw_offsets_before.txt"
kafka_end_offsets "$ANALYTICS_TOPIC" > "$OUT_REL/01_analytics_offsets_before.txt"
start_job phase1 0   # 0 = chạy đến khi bị kill
sleep "$RUN1"

checkpoint_state > "$OUT_REL/02_checkpoint_before_kill.txt"
kafka_end_offsets "$RAW_TOPIC" > "$OUT_REL/02_raw_offsets_at_kill.txt"
kafka_end_offsets "$ANALYTICS_TOPIC" > "$OUT_REL/02_analytics_offsets_at_kill.txt"

# ---------- Kill ----------
PID="$(job_pid)"
log "KILL -9 driver Spark (pid: ${PID:-không tìm thấy}) — mô phỏng sự cố, không shutdown êm"
"$DC" exec -i spark sh -lc "pkill -9 -f analytics_job.py || true"
wait_for_job_exit
log "query đã chết; producer vẫn gửi, chờ ${GAP}s để Kafka dồn dữ liệu chưa xử lý"
sleep "$GAP"
kafka_end_offsets "$RAW_TOPIC" > "$OUT_REL/03_raw_offsets_after_gap.txt"

# ---------- Phase 2: khởi động lại với checkpoint cũ ----------
log "PHASE 2: khởi động lại CÙNG query, CÙNG checkpoint, chạy ${RUN2}s"
start_job phase2 "$RUN2"
sleep $((RUN2 + 60))
wait_for_job_exit

checkpoint_state > "$OUT_REL/04_checkpoint_after_restart.txt"
kafka_end_offsets "$RAW_TOPIC" > "$OUT_REL/04_raw_offsets_after.txt"
kafka_end_offsets "$ANALYTICS_TOPIC" > "$OUT_REL/04_analytics_offsets_after.txt"

# ---------- Tổng hợp ----------
cat > "$OUT_REL/README.md" <<EOF
# Bằng chứng crash recovery — $STAMP

| Tham số | Giá trị |
|---|---|
| Query | \`$QUERY\` |
| Checkpoint | \`$CHECKPOINT\` |
| Watermark | ${WATERMARK:-1 minute} |
| Chạy trước khi kill | ${RUN1}s |
| Thời gian query chết | ${GAP}s (producer vẫn gửi) |
| Chạy sau khi restart | ${RUN2}s |

## Cần đối chiếu

1. \`02_checkpoint_before_kill.txt\` — batch cuối trước khi chết và offset Kafka của nó.
2. \`phase2_spark.log\` + \`04_checkpoint_after_restart.txt\` — batch đầu sau restart
   phải bắt đầu từ đúng offset mà batch cuối đã commit, không nhảy tới cuối topic.
3. \`03_raw_offsets_after_gap.txt\` so với \`02_raw_offsets_at_kill.txt\` — chứng minh
   dữ liệu vẫn vào Kafka trong lúc Spark chết.
4. \`phase2_progress.json\` — \`sources[].startOffset\` của batch đầu sau restart và
   số dòng đã xử lý; \`04_analytics_offsets_after.txt\` cho thấy topic analytics tiếp tục tăng.

Guarantee quan sát được ở đây là Kafka source offset recovery qua checkpoint của Spark,
không phải exactly-once toàn pipeline. Xem docs/05_experiments_and_evaluation.md.
EOF

log "xong. Bằng chứng: $OUT_REL"
ls -1 "$OUT_REL"
