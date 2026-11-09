#!/usr/bin/env python3
"""Selfcheck cho dashboard: state và render, không cần Kafka.

Chạy:  python tests/dashboard_selfcheck.py
Chỉ dùng stdlib, nên chạy được trên máy host không cài confluent-kafka
(`confluent_kafka` chỉ được import bên trong các hàm consumer).
"""
import json
import os
import re
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


def body_noscript(page):
    """Chỉ phần <noscript> ở body. Khẳng định nội dung dựng sẵn phải nhắm vào đây: cùng một chuỗi
    có thể nằm trong _JS, nên tìm trên cả trang thì khẳng định nào cũng đúng mà chẳng chứng minh gì."""
    start = page.rindex("<noscript>") + len("<noscript>")
    return page[start:page.index("</noscript>", start)]


def head_noscript(page):
    """<noscript> đầu tiên, nằm trong <head>: meta refresh và CSS giấu phần khung động."""
    start = page.index("<noscript>") + len("<noscript>")
    return page[start:page.index("</noscript>", start)]


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
fallback = body_noscript(page)
assert "<script>alert(1)</script>" not in page
assert "&lt;script&gt;" in fallback
assert "Tranh cãi &amp; bàn luận" in fallback

# Nội dung thật sự xuất hiện trong khối dựng sẵn ở server, không phải trong chuỗi của _JS.
assert "enwiki" in fallback and "55" in fallback
assert "Human" in fallback and "80" in fallback

# window_start cũng đến từ Kafka nên cũng phải escape, không chỉ riêng title. Hai nhánh của
# _hhmm: chuỗi có 'T' bị cắt 5 ký tự, chuỗi không có 'T' đi nguyên văn ra pill ở header.
hostile = new_state()
update_state(hostile, {"metric": "edit_count", "wiki": "enwiki", "edit_count": 1,
                       "window_start": "2026-11-08T<img src=x onerror=alert(1)>"})
hostile_page = render_page(hostile, "12:00:00")
assert "<img" not in hostile_page and "&lt;img" in hostile_page, hostile_page[:400]

hostile2 = new_state()
update_state(hostile2, {"metric": "bot_ratio", "window_start": "<script>alert(1)</script>",
                        "total_events": 1, "bot_count": 1, "human_count": 0, "unknown_count": 0})
assert "<script>alert(1)</script>" not in render_page(hostile2, "12:00:00")

# Trang là shell + <noscript>: client lấy dữ liệu từ /api/state, không tải tài nguyên ngoài.
assert "<noscript>" in page
assert "/api/state" in page

# Phòng bảo vệ có thể không có mạng, nên không được có BẤT KỲ tài nguyên ngoài nào. Dùng regex
# để bắt cả nháy đơn, url(...) trong CSS, @import và fetch('http...'), không chỉ src=" / href=".
EXTERNAL = re.compile(r"""(?:src|href)\s*=\s*['"]?https?:"""
                      r"""|url\(\s*['"]?https?:"""
                      r"""|fetch\(\s*['"]?https?:"""
                      r"""|@import""", re.I)
assert not EXTERNAL.search(page), EXTERNAL.search(page).group(0)
assert not EXTERNAL.search(render_page(new_state(), "12:00:00"))

# Khi JavaScript tắt, phần khung động phải bị giấu, nếu không nó nằm cạnh khối dựng sẵn và
# trang trông như hỏng. CSS giấu nằm trong <noscript> ở <head>.
head_css = head_noscript(page)
assert "display:none" in head_css, head_css
for hook in ("#live", ".kpi", ".statusbar"):
    assert hook in head_css, head_css
    assert hook.lstrip("#.") in page  # và khung động thật sự mang các móc đó

# Trang rỗng vẫn render được và nói rõ vì sao chưa có gì — trong khối dựng sẵn, không phải
# trong một chuỗi nằm sẵn ở _JS.
empty = render_page(new_state(), "12:00:00")
assert "Chưa có window nào đóng" in body_noscript(empty)
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

# recent_spikes phải đếm số bản ghi thật sự vượt ngưỡng. Dòng thật ~30 spike mỗi 30 giây, nên
# 300 bản ghi là chuyện bình thường và KPI không được dừng ở bất kỳ cap nào của feed.
burst = new_state()
update_state(burst, edit("enwiki", 12, 1))  # cần một window timeline để có mốc "gần đây"
for i in range(300):
    update_state(burst, spike(f"Trang {i}"))
burst_json = state_json(burst, "12:00:00")
assert burst_json["kpi"]["recent_spikes"] == 300, burst_json["kpi"]
assert len(burst_json["spikes"]) == 12, len(burst_json["spikes"])  # feed vẫn chỉ hiện 12 cái mới nhất
assert burst_json["spikes"][0]["title"] == "Trang 299", burst_json["spikes"][0]

# Timeline rỗng thì chưa có mốc "gần đây" nào để so, nên KPI phải là 0 chứ không phải cộng sạch
# mọi window đã tally — vài giây đầu sau khi khởi động lại đúng vào trường hợp này.
blank = new_state()
for i in range(5):
    update_state(blank, spike(f"Trang {i}"))
assert state_json(blank, "12:00:00")["kpi"]["recent_spikes"] == 0, state_json(blank, "12:00:00")["kpi"]

# Chỉ spike từ cutoff trở đi mới được đếm; cutoff là window timeline cũ nhất trong 10 cái gần nhất.
old = new_state()
for minute in range(9, 14):
    update_state(old, {"metric": "activity_spike", "window_start": f"2026-11-08T10:{minute:02d}:00.000Z"})
for minute in range(10, 14):
    update_state(old, edit("enwiki", minute, 5))
assert state_json(old, "12:00:00")["kpi"]["recent_spikes"] == 4, old["spike_counts"]  # bỏ cái 10:09

# Tally cắt bớt window cũ nên không phình vô hạn dù chạy cả buổi.
for minute in range(200):
    update_state(old, {"metric": "activity_spike",
                       "window_start": f"2026-11-08T{minute // 60:02d}:{minute % 60:02d}:00.000Z"})
assert len(old["spike_counts"]) == 120, len(old["spike_counts"])

# state_json: serialize được (deque thì không), đủ khóa, không lộ comment/tên gốc.
payload = state_json(state, "12:00:00")
assert set(payload) == {"kpi", "timeline", "wikis", "bot_history", "spikes", "log", "status"}
assert json.dumps(payload)
assert payload["kpi"]["total_records"] == state["total"]
assert payload["log"][0]["title"] == "Second"
assert payload["status"]["last_window"] == state["last_window"]

# Quyền riêng tư đi hết đường: một event thật có cả `comment` lẫn tên gốc, qua log_entry và
# update_log, thì /api/state không được chứa chữ nào của hai thứ đó.
update_log(state, log_entry(raw(user="Printstream", comment="toi vua sua lai cai bang"), WIKIS, KINDS))
served = json.dumps(state_json(state, "12:00:00"), ensure_ascii=False)
assert "Printstream" not in served
assert "toi vua sua lai cai bang" not in served

assert state_json(new_state(), "12:00:00")["kpi"]["emit_lag_seconds"] is None  # state rỗng không nổ

print("DASHBOARD SELFCHECK OK")
