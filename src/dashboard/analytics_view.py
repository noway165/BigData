"""Biến message của topic `wikipedia-analytics` thành bảng cho Dashboard.

Tách khỏi app.py để test được bằng pytest mà không cần Streamlit hay Kafka.

Hợp đồng input (do src/streaming/analytics_job.py phát):
  key   = tên metric: edit_count | bot_ratio | activity_spike
  value = JSON object, có window_start, window_end, emitted_at và các trường của metric.

Kafka sink của Spark là at-least-once: khi một batch chạy lại sau sự cố, cùng một
window có thể được phát hai lần. Dashboard vì vậy khử trùng theo khóa tự nhiên của
từng metric và giữ bản phát sau cùng (emitted_at lớn nhất).
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field

import pandas as pd

METRICS = ("edit_count", "bot_ratio", "activity_spike")

# Khóa tự nhiên của một dòng output, dùng để khử bản ghi lặp.
NATURAL_KEYS = {
    "edit_count": ["window_start", "wiki"],
    "bot_ratio": ["window_start"],
    "activity_spike": ["window_start", "wiki", "title"],
}

TIME_COLUMNS = ("window_start", "window_end", "emitted_at")


@dataclass
class ParseStats:
    received: int = 0
    bad_json: int = 0
    unknown_metric: int = 0
    per_metric: dict = field(default_factory=lambda: {m: 0 for m in METRICS})


def parse_message(key, value, stats: ParseStats | None = None) -> dict | None:
    """Một message Kafka thành dict, hoặc None nếu hỏng. Không ném lỗi: một message
    hỏng không được làm sập Dashboard, nhưng phải được đếm."""
    stats = stats if stats is not None else ParseStats()
    stats.received += 1
    try:
        if isinstance(value, (bytes, bytearray)):
            value = value.decode("utf-8")
        record = json.loads(value)
        if not isinstance(record, dict):
            raise ValueError("value không phải JSON object")
    except (ValueError, UnicodeDecodeError, TypeError):
        stats.bad_json += 1
        return None

    if isinstance(key, (bytes, bytearray)):
        key = key.decode("utf-8", errors="replace")
    metric = record.get("metric") or key
    if metric not in METRICS:
        stats.unknown_metric += 1
        return None
    record["metric"] = metric
    stats.per_metric[metric] += 1
    return record


def to_frames(records: list[dict]) -> dict[str, pd.DataFrame]:
    """Gom record theo metric, đổi kiểu thời gian và khử trùng."""
    frames = {}
    for metric in METRICS:
        rows = [r for r in records if r.get("metric") == metric]
        df = pd.DataFrame(rows)
        if df.empty:
            frames[metric] = df
            continue
        for col in TIME_COLUMNS:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")
        df = df.dropna(subset=["window_start"])
        keys = [k for k in NATURAL_KEYS[metric] if k in df.columns]
        sort_cols = ["emitted_at"] if "emitted_at" in df.columns else []
        if sort_cols:
            df = df.sort_values(sort_cols, kind="stable")
        df = df.drop_duplicates(subset=keys, keep="last")
        frames[metric] = df.sort_values(keys).reset_index(drop=True)
    return frames


def duplicates_removed(records: list[dict], frames: dict[str, pd.DataFrame]) -> int:
    """Số bản ghi lặp đã bị khử, để hiển thị cho người xem biết at-least-once có xảy ra."""
    total = sum(1 for r in records if r.get("metric") in METRICS)
    return total - sum(len(df) for df in frames.values())


# ---------- Các bảng/biểu đồ của Dashboard ----------

def latest_window(df: pd.DataFrame) -> pd.Timestamp | None:
    if df is None or df.empty:
        return None
    return df["window_start"].max()


def edits_timeline(edit_df: pd.DataFrame) -> pd.DataFrame:
    """Tổng số event mỗi phút (cộng mọi wiki), theo event time."""
    if edit_df.empty:
        return pd.DataFrame(columns=["window_start", "edit_count", "bot_edits", "non_bot_edits"])
    out = (edit_df.groupby("window_start", as_index=False)[["edit_count", "bot_edits"]].sum()
           .sort_values("window_start"))
    out["non_bot_edits"] = out["edit_count"] - out["bot_edits"]
    return out


def latest_total(edit_df: pd.DataFrame) -> tuple[pd.Timestamp | None, int, int | None]:
    """(window mới nhất, tổng event của window đó, chênh lệch so với window trước)."""
    timeline = edits_timeline(edit_df)
    if timeline.empty:
        return None, 0, None
    last = timeline.iloc[-1]
    delta = None
    if len(timeline) > 1:
        delta = int(last["edit_count"] - timeline.iloc[-2]["edit_count"])
    return last["window_start"], int(last["edit_count"]), delta


def top_wikis(edit_df: pd.DataFrame, last_n_windows: int = 10, top: int = 10) -> pd.DataFrame:
    """Top wiki theo tổng event trong N window 1 phút gần nhất đã đóng."""
    if edit_df.empty:
        return pd.DataFrame(columns=["wiki", "edit_count", "bot_edits", "bot_share"])
    windows = sorted(edit_df["window_start"].unique())[-last_n_windows:]
    recent = edit_df[edit_df["window_start"].isin(windows)]
    out = (recent.groupby("wiki", as_index=False)[["edit_count", "bot_edits"]].sum()
           .sort_values(["edit_count", "wiki"], ascending=[False, True]).head(top))
    out["bot_share"] = (out["bot_edits"] / out["edit_count"]).round(4)
    return out.reset_index(drop=True)


def bot_breakdown(bot_df: pd.DataFrame) -> pd.DataFrame:
    """Bot/Human/Unknown của window 5 phút mới nhất.

    Mẫu số là total_events của chính window đó, kể cả Unknown (giống analytics_job).
    """
    cols = ["category", "count", "share"]
    if bot_df.empty:
        return pd.DataFrame(columns=cols)
    last = bot_df.sort_values("window_start").iloc[-1]
    total = int(last["total_events"]) or 0
    rows = []
    for category, col in (("Bot", "bot_count"), ("Human", "human_count"), ("Unknown", "unknown_count")):
        count = int(last.get(col, 0) or 0)
        rows.append({"category": category, "count": count,
                     "share": round(count / total, 4) if total else 0.0})
    return pd.DataFrame(rows, columns=cols)


def bot_ratio_history(bot_df: pd.DataFrame) -> pd.DataFrame:
    """Tỷ lệ ba nhóm qua các window, dạng long cho biểu đồ."""
    if bot_df.empty:
        return pd.DataFrame(columns=["window_start", "category", "share"])
    long = bot_df.melt(id_vars=["window_start"],
                       value_vars=["bot_share", "human_share", "unknown_share"],
                       var_name="category", value_name="share")
    long["category"] = long["category"].map(
        {"bot_share": "Bot", "human_share": "Human", "unknown_share": "Unknown"})
    return long.sort_values(["window_start", "category"]).reset_index(drop=True)


def spike_table(spike_df: pd.DataFrame, last_minutes: int = 30) -> pd.DataFrame:
    """Bài viết có hoạt động chỉnh sửa tăng đột biến, gộp các window trượt chồng nhau.

    Một đợt sửa dồn dập của một bài nằm trong tối đa 4 window trượt (2 phút / 30 giây),
    nên gộp theo (wiki, title) và giữ window có edit_count cao nhất để bảng không lặp.
    """
    cols = ["wiki", "title", "edit_count", "distinct_users", "bot_edits", "human_edits",
            "window_start", "window_end", "windows_flagged", "likely_bot_driven"]
    if spike_df.empty:
        return pd.DataFrame(columns=cols)
    newest = spike_df["window_end"].max()
    recent = spike_df[spike_df["window_end"] >= newest - pd.Timedelta(minutes=last_minutes)]
    recent = recent.sort_values(["edit_count", "window_start"], ascending=[False, False])
    best = recent.drop_duplicates(subset=["wiki", "title"], keep="first").copy()
    counts = recent.groupby(["wiki", "title"]).size().rename("windows_flagged")
    best = best.join(counts, on=["wiki", "title"])
    # Hơn một nửa số lượt sửa là bot: đột biến này nhiều khả năng là bot chạy hàng loạt,
    # không phải tranh chấp giữa người dùng.
    best["likely_bot_driven"] = best["bot_edits"] > best["edit_count"] / 2
    return best[cols].reset_index(drop=True)


def result_delay_seconds(frames: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """emitted_at - window_end: window đóng xong bao lâu thì kết quả tới Kafka.

    Gồm thời gian chờ watermark và chờ trigger. Không phải end-to-end latency của
    từng event (cái đó đo trên raw Parquet, xem latency_report.md).
    """
    rows = []
    for metric, df in frames.items():
        if df.empty or "emitted_at" not in df.columns:
            continue
        delay = (df["emitted_at"] - df["window_end"]).dt.total_seconds().dropna()
        if delay.empty:
            continue
        rows.append({"metric": metric, "samples": int(delay.size),
                     "p50_s": round(float(delay.quantile(0.5)), 1),
                     "p95_s": round(float(delay.quantile(0.95)), 1)})
    return pd.DataFrame(rows, columns=["metric", "samples", "p50_s", "p95_s"])
