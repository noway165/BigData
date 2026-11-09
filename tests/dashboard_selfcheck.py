#!/usr/bin/env python3
"""Selfcheck cho dashboard: state và render, không cần Kafka.

Chạy:  python tests/dashboard_selfcheck.py
Chỉ dùng stdlib, nên chạy được trên máy host không cài confluent-kafka
(`confluent_kafka` chỉ được import bên trong các hàm consumer).
"""
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src", "dashboard"))

from dashboard import (LOG_LINES, log_entry, mask_user, new_state, render_page, state_json,  # noqa: E402
                       update_log, update_state)


def edit(wiki, window, count, bots=0):
    return {"metric": "edit_count", "wiki": wiki, "edit_count": count, "bot_edits": bots,
            "window_start": f"2026-11-08T10:{window}:00.000Z",
            "window_end": f"2026-11-08T10:{window:02d}:59.999Z", "counted_types": "all"}


def ratio(window, bot, human, unknown):
    total = bot + human + unknown
    return {"metric": "bot_ratio", "total_events": total, "bot_count": bot,
            "human_count": human, "unknown_count": unknown,
            "bot_share": round(bot / total, 4), "human_share": round(human / total, 4),
            "unknown_share": round(unknown / total, 4),
            "window_start": f"2026-11-08T10:{window}:00.000Z",
            "window_end": f"2026-11-08T10:{window + 5:02d}:00.000Z", "counted_types": "all"}


def spike(title, count=14, users=5):
    return {"metric": "activity_spike", "wiki": "enwiki", "title": title, "edit_count": count,
            "distinct_users": users, "bot_edits": 1, "human_edits": count - 1,
            "threshold_edits": 10, "threshold_distinct_users": 0,
            "window_start": "2026-11-08T10:12:00.000Z", "window_end": "2026-11-08T10:14:00.000Z"}


state = new_state()

# Window mới hơn thay thế window cũ hơn của cùng wiki.
update_state(state, edit("enwiki", 10, 40))
update_state(state, edit("enwiki", 11, 55))
assert state["edit_count"]["enwiki"]["edit_count"] == 55, state["edit_count"]["enwiki"]

# Bản ghi đến muộn của window cũ KHÔNG được ghi đè window mới: Kafka không bảo đảm
# thứ tự giữa các partition.
update_state(state, edit("enwiki", 10, 999))
assert state["edit_count"]["enwiki"]["edit_count"] == 55, state["edit_count"]["enwiki"]

# Timeline cộng mọi wiki trong cùng window.
update_state(state, edit("dewiki", 11, 20))
assert state["timeline"]["2026-11-08T10:11:00.000Z"] == 75, state["timeline"]
assert state["timeline"]["2026-11-08T10:10:00.000Z"] == 40 + 999, state["timeline"]

# bot_ratio giữ window mới nhất, bản ghi cũ đến sau không ghi đè.
update_state(state, ratio(10, 30, 60, 10))
update_state(state, ratio(15, 10, 80, 10))
update_state(state, ratio(10, 1, 1, 1))
assert state["bot_ratio"]["window_start"] == "2026-11-08T10:15:00.000Z", state["bot_ratio"]
assert state["bot_ratio"]["bot_count"] == 10, state["bot_ratio"]

# Metric lạ bị bỏ qua, không làm hỏng state.
update_state(state, {"metric": "khong_ton_tai", "window_start": "2026-11-08T10:20:00.000Z"})
update_state(state, {})
assert state["ignored"] == 2, state["ignored"]
assert state["total"] == 7, state["total"]
assert state["last_window"] == "2026-11-08T10:15:00.000Z", state["last_window"]

# Tiêu đề trang Wikipedia có thể chứa HTML; phải escape, nếu không trang vỡ.
update_state(state, spike("<script>alert(1)</script>"))
update_state(state, spike("Tranh cãi & bàn luận"))
page = render_page(state, "12:00:00")
assert "<script>alert(1)</script>" not in page
assert "&lt;script&gt;" in page
assert "Tranh cãi &amp; bàn luận" in page

# Nội dung thật sự xuất hiện trên trang.
assert "enwiki" in page and "55" in page
assert "Human" in page and "80" in page

# Trang rỗng vẫn render được và nói rõ vì sao chưa có gì.
empty = render_page(new_state(), "12:00:00")
assert "Chưa có window nào đóng" in empty
assert "<html" in empty

# Che tên người dùng: tên thường giữ 3 ký tự đầu, IPv4/IPv6 giữ phần đầu, rỗng thì ẩn danh.
assert mask_user("Printstream") == "Pri***"
assert mask_user("Ab") == "A***"
assert mask_user("171.224.3.9") == "171.224.*.*"
assert mask_user("2001:db8:85a3::8a2e") == "2001:db8:*"
assert mask_user("Abc") == "A***"  # đúng 3 ký tự thì không được lộ nguyên tên
assert mask_user("User:Foo") == "Use***"  # một dấu ':' không phải IPv6
assert mask_user(None) == "ẩn danh"
assert mask_user("") == "ẩn danh"

WIKIS, KINDS = frozenset({"enwiki", "viwiki"}), frozenset({"edit", "new"})


def raw(**over):
    event = {"wiki": "enwiki", "type": "edit", "title": "Ranjeev Puri", "user": "ABlitzz",
             "bot": False, "meta": {"dt": "2026-11-09T16:18:52Z"},
             "length": {"old": 1200, "new": 1612}}
    event.update(over)
    return event


entry = log_entry(raw(), WIKIS, KINDS)
assert entry["time"] == "16:18:52"
assert entry["user"] == "ABl***"
assert entry["kind"] == "edit" and entry["delta"] == 412 and entry["bot"] is False

# Wiki/loại ngoài bộ lọc hoặc thiếu meta.dt thì bỏ; `comment` (văn bản tự do của người lạ) không bao giờ vào entry.
assert log_entry(raw(wiki="commonswiki"), WIKIS, KINDS) is None
assert log_entry(raw(type="categorize"), WIKIS, KINDS) is None
assert log_entry(raw(type="log"), WIKIS, KINDS) is None
assert log_entry(raw(meta={}), WIKIS, KINDS) is None
assert log_entry(raw(user=None), WIKIS, KINDS)["user"] == "ẩn danh"
assert log_entry(raw(type="new", length={"new": 800}), WIKIS, KINDS)["delta"] == 800
assert log_entry(raw(length={}), WIKIS, KINDS)["delta"] is None
assert log_entry(raw(bot=True), WIKIS, KINDS)["bot"] is True
assert "comment" not in log_entry(raw(comment="xin chao"), WIKIS, KINDS)
assert log_entry(raw(title="A & <b>"), WIKIS, KINDS)["title"] == "A & <b>"  # escape thuộc về renderer
assert "ABlitzz" not in str(entry)  # tên gốc không lọt vào entry

# wiki_split: window mới nhất theo wiki, human_edits suy ra, window cũ đến muộn bị bỏ.
state = new_state()
update_state(state, edit("enwiki", 11, 55, bots=20))
assert state["wiki_split"]["enwiki"] == {"edit_count": 55, "bot_edits": 20, "human_edits": 35,
                                         "window_start": "2026-11-08T10:11:00.000Z"}
update_state(state, edit("enwiki", 10, 999, bots=1))
assert state["wiki_split"]["enwiki"]["edit_count"] == 55, state["wiki_split"]

# bot_history: mỗi window một bản ghi mới nhất, cắt còn BOT_HISTORY_WINDOWS window lớn nhất.
update_state(state, ratio(10, 30, 60, 10))
update_state(state, ratio(15, 10, 80, 10))
assert sorted(state["bot_history"]) == ["2026-11-08T10:10:00.000Z", "2026-11-08T10:15:00.000Z"]
update_state(state, ratio(10, 1, 1, 1))
assert state["bot_history"]["2026-11-08T10:10:00.000Z"]["bot_count"] == 1
for minute in range(20, 40):
    update_state(state, ratio(minute, 1, 1, 1))
assert len(state["bot_history"]) == 12 and "2026-11-08T10:10:00.000Z" not in state["bot_history"]

# log: mới nhất đứng đầu, giới hạn LOG_LINES.
update_log(state, {"time": "16:00:01", "wiki": "enwiki", "user": "A***", "bot": False,
                   "kind": "edit", "title": "First", "delta": 10})
update_log(state, {"time": "16:00:02", "wiki": "viwiki", "user": "B***", "bot": True,
                   "kind": "new", "title": "Second", "delta": None})
assert [e["title"] for e in state["log"]] == ["Second", "First"]
assert state["log"].maxlen == LOG_LINES  # không gắn cứng 60: DASHBOARD_LOG_LINES có thể đổi nó

# Đẩy quá giới hạn thì dòng cũ nhất bị loại, dòng mới nhất vẫn đứng đầu.
capped = new_state()
for i in range(LOG_LINES + 1):
    update_log(capped, {"title": f"t{i}"})
assert len(capped["log"]) == LOG_LINES
assert capped["log"][0]["title"] == f"t{LOG_LINES}" and "t0" not in [e["title"] for e in capped["log"]]

# human_edits không âm dù bản ghi báo bot_edits > edit_count.
update_state(state, edit("frwiki", 11, 5, bots=9))
assert state["wiki_split"]["frwiki"]["human_edits"] == 0, state["wiki_split"]["frwiki"]

# state_json: serialize được (deque thì không), đủ khóa, không lộ comment/tên gốc.
payload = state_json(state, "12:00:00")
assert set(payload) == {"kpi", "timeline", "wikis", "bot_history", "spikes", "log", "status"}
assert json.dumps(payload)
assert payload["kpi"]["total_records"] == state["total"]
assert payload["log"][0]["title"] == "Second"
assert all("comment" not in e for e in payload["log"])
assert payload["status"]["last_window"] == state["last_window"]
assert state_json(new_state(), "12:00:00")["kpi"]["emit_lag_seconds"] is None  # state rỗng không nổ

print("DASHBOARD SELFCHECK OK")
