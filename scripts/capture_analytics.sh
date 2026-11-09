#!/bin/sh
# Lưu output thật của topic analytics thành JSONL cho Dashboard chạy offline (dự phòng demo).
#   sh scripts/capture_analytics.sh [số message] [file]
#   ANALYTICS_SOURCE_FILE=<file> streamlit run src/dashboard/app.py
set -eu
MAX="${1:-5000}"
OUT="${2:-outputs/experiment_results/analytics_capture.jsonl}"
# --timeout-ms: hết message thì dừng thay vì chờ mãi.
sh "$(dirname "$0")/consume_analytics.sh" "$MAX" --timeout-ms 15000 2>/dev/null \
  | awk -F'\t' 'NF>=2 { printf("{\"key\":\"%s\",\"value\":%s}\n", $1, $2) }' > "$OUT" || true
echo "đã ghi $(wc -l < "$OUT") message vào $OUT"
