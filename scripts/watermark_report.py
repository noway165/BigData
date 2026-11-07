"""Gộp các file JSON của một lần chạy watermark_experiment.sh thành bảng Markdown.

Dùng: python scripts/watermark_report.py outputs/experiment_results/watermark_<stamp>
"""
import json
import sys
from pathlib import Path

ROWS = [
    ("Watermark", lambda c, s: c.get("watermark")),
    ("Event đầu vào", lambda c, s: s.get("input_rows")),
    ("Dòng bị watermark loại", lambda c, s: s.get("rows_dropped_by_watermark")),
    ("State rows lớn nhất", lambda c, s: s.get("max_state_rows")),
    ("State bytes lớn nhất", lambda c, s: s.get("max_state_bytes")),
    ("Batch duration p50 (ms)", lambda c, s: s.get("batch_ms_p50")),
    ("Batch duration p95 (ms)", lambda c, s: s.get("batch_ms_p95")),
    ("Số batch", lambda c, s: s.get("batches")),
    ("Watermark cuối", lambda c, s: s.get("last_watermark")),
]


def main(folder: str) -> int:
    # Console Windows mặc định cp1252, không in được tiếng Việt.
    sys.stdout.reconfigure(encoding="utf-8")
    runs = []
    for path in sorted(Path(folder).glob("*.json")):
        data = json.loads(path.read_text("utf-8"))
        if "summary" in data:
            runs.append((path.stem, data.get("config", {}), data["summary"]))
    if not runs:
        print(f"Không có file kết quả trong {folder}", file=sys.stderr)
        return 1

    print(f"# So sánh watermark — {Path(folder).name}\n")
    print("Cùng một mẫu dữ liệu được replay cho mọi cấu hình; mỗi cấu hình có checkpoint riêng.\n")
    print("| Chỉ số | " + " | ".join(name for name, _, _ in runs) + " |")
    print("|---" * (len(runs) + 1) + "|")
    for label, get in ROWS:
        print(f"| {label} | " + " | ".join(str(get(c, s)) for _, c, s in runs) + " |")

    cfg = runs[0][1]
    print(f"\nMetric: `{cfg.get('metrics')}`, trigger {cfg.get('trigger')}, "
          f"maxOffsetsPerTrigger {cfg.get('max_offsets_per_trigger')}, "
          f"mỗi cấu hình chạy {cfg.get('run_seconds', 'xem config')} giây.\n")
    print("Watermark không phải bộ lọc event cũ: nó quyết định khi nào Spark đóng window và "
          "bỏ state. Watermark lớn giữ state lâu hơn nên tốn bộ nhớ hơn, nhưng nhận được "
          "nhiều event đến trễ hơn.")
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    sys.exit(main(sys.argv[1]))
