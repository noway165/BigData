"""Test logic Dashboard (không cần Streamlit/Kafka). Chạy: python -m pytest tests -q

tests/sample_events/analytics_sample.jsonl là dữ liệu TỔNG HỢP (synthetic) theo đúng
schema output của analytics_job.py, dùng cho test và chạy Dashboard offline.
Không phải số liệu đo thật; số liệu thật lấy từ topic wikipedia-analytics.
"""
import json
import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src" / "dashboard"))

import analytics_view as av  # noqa: E402

SAMPLE = ROOT / "tests" / "sample_events" / "analytics_sample.jsonl"


def load_sample():
    stats = av.ParseStats()
    records = []
    for line in SAMPLE.read_text("utf-8").splitlines():
        item = json.loads(line)
        rec = av.parse_message(item["key"].encode(), json.dumps(item["value"]).encode(), stats)
        if rec:
            records.append(rec)
    return records, stats


def ts(s):
    return pd.Timestamp(s, tz="UTC")


def edit(window, wiki, count, bots=0, emitted="2026-11-07T19:00:00.000Z"):
    end = (ts(window) + pd.Timedelta(minutes=1)).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    return {"metric": "edit_count", "window_start": window, "window_end": end,
            "wiki": wiki, "edit_count": count, "bot_edits": bots, "emitted_at": emitted}


# ---------- parse ----------

def test_parse_counts_bad_and_unknown_messages():
    stats = av.ParseStats()
    assert av.parse_message(b"edit_count", b"{not json", stats) is None
    assert av.parse_message(b"edit_count", b"[1,2]", stats) is None
    assert av.parse_message(b"other", b'{"window_start": "x"}', stats) is None
    rec = av.parse_message(b"bot_ratio", b'{"window_start": "2026-11-07T18:40:00.000Z"}', stats)
    assert rec["metric"] == "bot_ratio"
    assert (stats.received, stats.bad_json, stats.unknown_metric) == (4, 2, 1)
    assert stats.per_metric["bot_ratio"] == 1


def test_metric_field_in_value_wins_over_key():
    rec = av.parse_message(None, json.dumps({"metric": "edit_count", "window_start": "x"}))
    assert rec["metric"] == "edit_count"


# ---------- khử trùng (Kafka sink at-least-once) ----------

def test_duplicate_window_keeps_latest_emission():
    recs = [edit("2026-11-07T18:40:00.000Z", "enwiki", 10, emitted="2026-11-07T18:42:00.000Z"),
            edit("2026-11-07T18:40:00.000Z", "enwiki", 12, emitted="2026-11-07T18:43:00.000Z"),
            edit("2026-11-07T18:40:00.000Z", "dewiki", 5)]
    frames = av.to_frames(recs)
    df = frames["edit_count"]
    assert len(df) == 2
    assert df.set_index("wiki").loc["enwiki", "edit_count"] == 12
    assert av.duplicates_removed(recs, frames) == 1


def test_sample_file_parses_and_dedupes():
    records, stats = load_sample()
    assert stats.bad_json == 0 and stats.unknown_metric == 0
    frames = av.to_frames(records)
    assert av.duplicates_removed(records, frames) == 1
    assert len(frames["edit_count"]) == 20 * 12
    assert len(frames["bot_ratio"]) == 4


# ---------- các bảng hiển thị ----------

def test_latest_total_and_delta():
    recs = [edit("2026-11-07T18:40:00.000Z", "enwiki", 10),
            edit("2026-11-07T18:40:00.000Z", "dewiki", 5),
            edit("2026-11-07T18:41:00.000Z", "enwiki", 7)]
    window, total, delta = av.latest_total(av.to_frames(recs)["edit_count"])
    assert window == ts("2026-11-07T18:41:00")
    assert total == 7 and delta == -8


def test_top_wikis_only_counts_recent_windows():
    recs = [edit("2026-11-07T18:40:00.000Z", "dewiki", 100),
            edit("2026-11-07T18:41:00.000Z", "enwiki", 10, bots=5),
            edit("2026-11-07T18:42:00.000Z", "enwiki", 10),
            edit("2026-11-07T18:42:00.000Z", "frwiki", 3)]
    top = av.top_wikis(av.to_frames(recs)["edit_count"], last_n_windows=2)
    assert list(top["wiki"]) == ["enwiki", "frwiki"]
    assert top.iloc[0]["edit_count"] == 20
    assert top.iloc[0]["bot_share"] == 0.25


def test_bot_breakdown_uses_window_total_including_unknown():
    rec = {"metric": "bot_ratio", "window_start": "2026-11-07T18:40:00.000Z",
           "window_end": "2026-11-07T18:45:00.000Z", "total_events": 10,
           "bot_count": 4, "human_count": 5, "unknown_count": 1,
           "bot_share": 0.4, "human_share": 0.5, "unknown_share": 0.1}
    b = av.bot_breakdown(av.to_frames([rec])["bot_ratio"]).set_index("category")
    assert list(b.index) == ["Bot", "Human", "Unknown"]
    assert b.loc["Unknown", "count"] == 1
    assert b["share"].sum() == pytest.approx(1.0)


def test_spike_table_merges_overlapping_windows_and_flags_bots():
    records, _ = load_sample()
    table = av.spike_table(av.to_frames(records)["activity_spike"], last_minutes=60)
    # Mỗi bài xuất hiện một lần dù nằm trong nhiều window trượt.
    assert not table.duplicated(["wiki", "title"]).any()
    row = table.set_index("title").loc["2026 Example Election results"]
    assert row["windows_flagged"] == 3 and row["edit_count"] == 16
    assert not row["likely_bot_driven"]
    assert table.set_index("title").loc["Q123456789", "likely_bot_driven"]


def test_empty_frames_do_not_crash():
    frames = av.to_frames([])
    assert av.latest_total(frames["edit_count"]) == (None, 0, None)
    assert av.top_wikis(frames["edit_count"]).empty
    assert av.bot_breakdown(frames["bot_ratio"]).empty
    assert av.spike_table(frames["activity_spike"]).empty
    assert av.result_delay_seconds(frames).empty


def test_timeline_marks_missing_minutes_but_latest_total_ignores_them():
    recs = [edit("2026-11-07T18:40:00.000Z", "enwiki", 10), edit("2026-11-07T18:42:00.000Z", "enwiki", 7)]
    df = av.to_frames(recs)["edit_count"]
    gaps = av.timeline_with_gaps(av.edits_timeline(df))
    assert len(gaps) == 3 and pd.isna(gaps.iloc[1]["edit_count"])
    assert av.latest_total(df)[1:] == (7, -3)
    assert av.timeline_with_gaps(av.edits_timeline(av.to_frames([])["edit_count"])).empty


@pytest.mark.parametrize("wiki,site", [
    ("enwiki", "en.wikipedia.org"), ("commonswiki", "commons.wikimedia.org"),
    ("wikidatawiki", "www.wikidata.org"), ("dewiktionary", "de.wiktionary.org"),
    ("zh_min_nanwiki", "zh-min-nan.wikipedia.org"),
])
def test_history_url_maps_wiki_codes(wiki, site):
    assert av.history_url(wiki, "A B/C") == f"https://{site}/w/index.php?title=A%20B%2FC&action=history"


def test_history_url_unknown_wiki_has_no_link():
    assert av.history_url("unknownsite", "X") is None


def test_pipeline_status_states():
    assert av.pipeline_status(10, None, 5) == ("kafka_down", 0)
    assert av.pipeline_status(None, 100, None) == ("waiting", 0)
    assert av.pipeline_status(10, 100, 50) == ("ok", 0)  # healthy: the difference means nothing
    assert av.pipeline_status(av.STALE_AFTER_S + 1, 100, 60) == ("stale", 40)
    assert av.pipeline_status(av.STALE_AFTER_S + 1, 100, None) == ("stale", 0)
    assert av.pipeline_status(av.STALE_AFTER_S + 1, 50, 60) == ("stale", 0)  # never negative


def test_result_delay_is_emitted_minus_window_end():
    recs = [edit("2026-11-07T18:40:00.000Z", "enwiki", 1, emitted="2026-11-07T18:42:10.000Z")]
    delay = av.result_delay_seconds(av.to_frames(recs))
    assert delay.iloc[0]["p50_s"] == 70.0
