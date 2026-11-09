#!/usr/bin/env python3
"""Dashboard tối thiểu cho topic `wikipedia-analytics`.

Đọc topic analytics, giữ state trong bộ nhớ và phục vụ một trang HTML tự refresh.

Không thêm dependency và không thêm image: chạy lại image của producer
(python:3.12-slim + confluent-kafka) với `http.server` của stdlib. Trang không
tải tài nguyên ngoài, nên buổi demo không cần mạng.

Mỗi lần khởi động dùng `group.id` mới và đọc từ `earliest`, nên trang hiện ngay
dữ liệu còn trong retention 24 giờ thay vì phải chờ window đầu tiên đóng. Đổi
`DASHBOARD_GROUP_ID` nếu muốn một consumer group bền và chỉ đọc phần mới.

Hợp đồng input: docs/02_architecture.md mục 5.
"""
import html
import json
import os
import threading
import uuid
from collections import deque
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.environ.get("KAFKA_ANALYTICS_TOPIC", "wikipedia-analytics")
PORT = int(os.environ.get("DASHBOARD_PORT", "8080"))
GROUP_ID = os.environ.get("DASHBOARD_GROUP_ID", "dashboard-" + uuid.uuid4().hex[:8])
REFRESH_S = int(os.environ.get("DASHBOARD_REFRESH_S", "5"))
TIMELINE_WINDOWS = int(os.environ.get("DASHBOARD_TIMELINE_WINDOWS", "20"))
TOP_WIKIS = int(os.environ.get("DASHBOARD_TOP_WIKIS", "12"))
RAW_TOPIC = os.environ.get("KAFKA_TOPIC", "wikipedia-recent-changes")
LOG_WIKIS = frozenset(w.strip() for w in os.environ.get("DASHBOARD_LOG_WIKIS", "enwiki,viwiki").split(",") if w.strip())
LOG_KINDS = frozenset({"edit", "new"})
LOG_LINES = int(os.environ.get("DASHBOARD_LOG_LINES", "60"))
BOT_HISTORY_WINDOWS = int(os.environ.get("DASHBOARD_BOT_HISTORY_WINDOWS", "12"))
METRICS = ("edit_count", "bot_ratio", "activity_spike")


def new_state() -> dict:
    return {
        "edit_count": {},      # wiki -> bản ghi của window mới nhất
        "timeline": {},        # window_start -> tổng lượt sửa mọi wiki
        "bot_ratio": None,     # bản ghi window mới nhất
        "wiki_split": {},      # wiki -> edit_count/bot_edits/human_edits của window mới nhất
        "bot_history": {},     # window_start -> bản ghi bot_ratio mới nhất của window đó
        "log": deque(maxlen=LOG_LINES),  # dòng log, mới nhất đứng đầu
        "spikes": deque(maxlen=30),
        "total": 0,
        "ignored": 0,
        "last_window": "",
    }


def _trim(mapping: dict, keep: int) -> None:
    """Giữ `keep` key lớn nhất. window_start là chuỗi ISO cùng định dạng nên so sánh chuỗi ra đúng thứ tự thời gian."""
    if len(mapping) <= keep:
        return
    for key in sorted(mapping)[:len(mapping) - keep]:
        del mapping[key]


def update_state(state: dict, rec: dict) -> dict:
    """Nạp một bản ghi analytics vào state. Bản ghi lạ bị bỏ qua, không làm hỏng trang."""
    metric = rec.get("metric")
    if metric not in METRICS:
        state["ignored"] += 1
        return state
    window_start = rec.get("window_start") or ""
    state["total"] += 1
    state["last_window"] = max(state["last_window"], window_start)

    if metric == "edit_count":
        wiki = rec.get("wiki") or "?"
        current = state["edit_count"].get(wiki)
        # Bản ghi của window cũ hơn không được ghi đè window mới hơn: Kafka không
        # bảo đảm thứ tự giữa các partition.
        if current is None or window_start >= (current.get("window_start") or ""):
            state["edit_count"][wiki] = rec
            count, bots = int(rec.get("edit_count") or 0), int(rec.get("bot_edits") or 0)
            state["wiki_split"][wiki] = {"edit_count": count, "bot_edits": bots,
                                         "human_edits": max(0, count - bots), "window_start": window_start}
        timeline = state["timeline"]
        timeline[window_start] = timeline.get(window_start, 0) + int(rec.get("edit_count") or 0)
        _trim(timeline, TIMELINE_WINDOWS)
    elif metric == "bot_ratio":
        current = state["bot_ratio"]
        if current is None or window_start >= (current.get("window_start") or ""):
            state["bot_ratio"] = rec
        state["bot_history"][window_start] = rec
        _trim(state["bot_history"], BOT_HISTORY_WINDOWS)
    else:
        state["spikes"].append(rec)
    return state


def update_log(state: dict, entry: dict) -> dict:
    """Đẩy một dòng log (đã qua log_entry) lên đầu; deque tự bỏ dòng cũ nhất khi đầy."""
    state["log"].appendleft(entry)
    return state


def mask_user(name) -> str:
    """Che tên người dùng trước khi lên màn chiếu. IP giữ phần mạng, tên thường giữ vài ký tự đầu."""
    if not name:
        return "ẩn danh"
    parts = name.split(".")
    if len(parts) == 4 and all(p.isdigit() for p in parts):
        return f"{parts[0]}.{parts[1]}.*.*"
    if name.count(":") >= 2:  # IPv6 mới có từ 2 dấu ':'; "User:Foo" thì không
        return ":".join(name.split(":")[:2]) + ":*"
    return name[:3] + "***" if len(name) > 3 else name[:1] + "***"


def log_entry(event: dict, wikis: frozenset, kinds: frozenset):
    """Một event Wikimedia -> dòng log, hoặc None nếu ngoài bộ lọc / thiếu meta.dt.

    Cố ý không mang `comment` (văn bản tự do của người lạ) và không escape: escape là việc của renderer.
    """
    dt = (event.get("meta") or {}).get("dt")
    if event.get("wiki") not in wikis or event.get("type") not in kinds or not dt:
        return None
    length = event.get("length") or {}
    new_len = length.get("new")
    return {
        "time": dt.split("T", 1)[-1][:8],
        "wiki": event["wiki"],
        "user": mask_user(event.get("user")),
        "bot": bool(event.get("bot")),
        "kind": event["type"],
        "title": event.get("title") or "",
        "delta": None if new_len is None else new_len - (length.get("old") or 0),
    }


def _parse_ts(iso):
    try:
        return datetime.fromisoformat(iso.replace("Z", "+00:00"))
    except (AttributeError, ValueError):
        return None


def state_json(state: dict, now: str) -> dict:
    """View model cho /api/state. Chỉ trả list/dict thuần để json.dumps được; gọi trong `with lock`."""
    timeline = sorted(state["timeline"].items())
    wikis = sorted(state["wiki_split"].items(), key=lambda kv: kv[1]["edit_count"], reverse=True)[:TOP_WIKIS]
    cutoff = timeline[-10:][0][0] if timeline else ""
    newest = max(list(state["edit_count"].values()) + ([state["bot_ratio"]] if state["bot_ratio"] else []),
                 key=lambda r: r.get("window_end") or "", default=None)
    lag = None
    emitted, ended = _parse_ts((newest or {}).get("emitted_at")), _parse_ts((newest or {}).get("window_end"))
    if emitted and ended:
        lag = round((emitted - ended).total_seconds(), 1)
    bot = state["bot_ratio"] or {}
    return {
        "kpi": {
            "edits_per_minute": timeline[-1][1] if timeline else 0,
            "bot_share": bot.get("bot_share"),
            "active_wikis": len(state["edit_count"]),
            "recent_spikes": sum(1 for r in state["spikes"] if (r.get("window_start") or "") >= cutoff),
            "emit_lag_seconds": lag,
            "total_records": state["total"],
        },
        "timeline": [{"window_start": ws, "edit_count": v} for ws, v in timeline],
        "wikis": [{"wiki": w, **v} for w, v in wikis],
        "bot_history": [dict(r) for _, r in sorted(state["bot_history"].items())],
        "spikes": [dict(r) for r in reversed(state["spikes"])],
        "log": [dict(e) for e in state["log"]],
        "status": {"now": now, "last_window": state["last_window"], "ignored": state["ignored"]},
    }


def _hhmm(iso: str) -> str:
    """'2026-11-07T18:48:00.000Z' -> '18:48'. Chuỗi lạ thì trả nguyên văn."""
    if not iso or "T" not in iso:
        return iso or "—"
    return iso.split("T", 1)[1][:5]


def _bar(ratio: float, color: str) -> str:
    width = max(0.0, min(1.0, ratio)) * 100
    return f'<div class="bar"><span style="width:{width:.1f}%;background:{color}"></span></div>'


def _esc(value) -> str:
    return html.escape(str(value), quote=True)


def render_page(state: dict, now: str) -> str:
    """Trang tĩnh, không JavaScript, không tài nguyên ngoài."""
    edits = sorted(state["edit_count"].values(),
                   key=lambda r: int(r.get("edit_count") or 0), reverse=True)[:TOP_WIKIS]
    max_edits = max([int(r.get("edit_count") or 0) for r in edits], default=0)

    if edits:
        rows = []
        for rec in edits:
            count = int(rec.get("edit_count") or 0)
            bots = int(rec.get("bot_edits") or 0)
            rows.append(
                f"<tr><td class='k'>{_esc(rec.get('wiki') or '?')}</td>"
                f"<td class='chart'>{_bar(count / max_edits if max_edits else 0, 'var(--accent)')}</td>"
                f"<td class='n'>{count}</td><td class='n dim'>{bots} bot</td>"
                f"<td class='n dim'>{_hhmm(rec.get('window_start') or '')}</td></tr>")
        edit_block = "<table>" + "".join(rows) + "</table>"
    else:
        edit_block = "<p class='empty'>Chưa có window nào đóng. outputMode append chỉ phát khi window đóng, nên chờ qua window + watermark.</p>"

    timeline = sorted(state["timeline"].items())
    if timeline:
        peak = max(v for _, v in timeline)
        bars = "".join(
            f'<div class="col" title="{_esc(ws)}: {v}">'
            f'<span style="height:{(v / peak * 100) if peak else 0:.1f}%"></span>'
            f'<label>{_hhmm(ws)}</label></div>'
            for ws, v in timeline)
        timeline_block = f'<div class="timeline">{bars}</div>'
    else:
        timeline_block = "<p class='empty'>Chưa có dữ liệu theo thời gian.</p>"

    bot = state["bot_ratio"]
    if bot:
        total = int(bot.get("total_events") or 0)
        parts = [("Bot", int(bot.get("bot_count") or 0), "var(--bot)"),
                 ("Human", int(bot.get("human_count") or 0), "var(--human)"),
                 ("Unknown", int(bot.get("unknown_count") or 0), "var(--unknown)")]
        segs = "".join(
            f'<span style="width:{(c / total * 100) if total else 0:.2f}%;background:{color}" '
            f'title="{label}: {c}"></span>' for label, c, color in parts)
        legend = "".join(
            f'<li><i style="background:{color}"></i>{label} <b>{c}</b> '
            f'({(c / total * 100) if total else 0:.1f}%)</li>' for label, c, color in parts)
        bot_block = (f'<div class="stack">{segs}</div><ul class="legend">{legend}</ul>'
                     f'<p class="dim">Window {_hhmm(bot.get("window_start") or "")}'
                     f'–{_hhmm(bot.get("window_end") or "")} UTC · {total} event</p>')
    else:
        bot_block = "<p class='empty'>Chưa có window bot_ratio nào đóng (tumbling 5 phút, nên chờ lâu nhất).</p>"

    spikes = list(state["spikes"])[::-1][:12]
    if spikes:
        rows = "".join(
            f"<tr><td class='k'>{_esc(rec.get('wiki') or '?')}</td>"
            f"<td>{_esc(rec.get('title') or '?')}</td>"
            f"<td class='n'>{_esc(rec.get('edit_count'))}</td>"
            f"<td class='n dim'>{_esc(rec.get('distinct_users'))} user</td>"
            f"<td class='n dim'>&gt;{_esc(rec.get('threshold_edits'))}</td>"
            f"<td class='n dim'>{_hhmm(rec.get('window_start') or '')}</td></tr>" for rec in spikes)
        spike_block = ("<table><thead><tr><th>Wiki</th><th>Trang</th><th class='n'>Lượt sửa</th>"
                       "<th class='n'>Người sửa</th><th class='n'>Ngưỡng</th><th class='n'>Window</th>"
                       f"</tr></thead>{rows}</table>")
    else:
        spike_block = "<p class='empty'>Chưa có window nào vượt ngưỡng.</p>"

    return f"""<!doctype html>
<html lang="vi"><head><meta charset="utf-8">
<meta http-equiv="refresh" content="{REFRESH_S}">
<title>Wikipedia Realtime Analytics</title>
<style>
:root {{ --bg:#0f1115; --card:#171a21; --line:#272b35; --fg:#e7e9ee; --dim:#9aa1ae;
        --accent:#4c8dff; --bot:#f0a73a; --human:#48c78e; --unknown:#6b7280; }}
* {{ box-sizing:border-box }}
body {{ margin:0; padding:24px; background:var(--bg); color:var(--fg);
        font:14px/1.5 -apple-system,Segoe UI,Roboto,sans-serif }}
header {{ display:flex; align-items:baseline; gap:16px; flex-wrap:wrap; margin-bottom:20px }}
h1 {{ font-size:20px; margin:0 }}
h2 {{ font-size:14px; margin:0 0 12px; color:var(--dim); font-weight:600;
      text-transform:uppercase; letter-spacing:.06em }}
.grid {{ display:grid; gap:16px; grid-template-columns:repeat(auto-fit,minmax(340px,1fr)) }}
.card {{ background:var(--card); border:1px solid var(--line); border-radius:10px; padding:16px 18px }}
.card.wide {{ grid-column:1/-1 }}
table {{ width:100%; border-collapse:collapse }}
td,th {{ padding:5px 8px; border-bottom:1px solid var(--line); text-align:left; vertical-align:middle }}
th {{ color:var(--dim); font-weight:600; font-size:12px }}
td.k {{ font-family:ui-monospace,Consolas,monospace; white-space:nowrap }}
td.n,th.n {{ text-align:right; white-space:nowrap; font-variant-numeric:tabular-nums }}
.dim {{ color:var(--dim) }}
.chart {{ width:45% }}
.bar {{ background:#232733; border-radius:3px; height:9px; overflow:hidden }}
.bar span {{ display:block; height:100% }}
.stack {{ display:flex; height:22px; border-radius:5px; overflow:hidden; background:#232733 }}
.stack span {{ display:block }}
.legend {{ list-style:none; display:flex; gap:18px; padding:0; margin:12px 0 4px; flex-wrap:wrap }}
.legend i {{ display:inline-block; width:10px; height:10px; border-radius:2px; margin-right:6px }}
.timeline {{ display:flex; align-items:flex-end; gap:4px; height:140px }}
.timeline .col {{ flex:1; display:flex; flex-direction:column; justify-content:flex-end;
                  align-items:center; height:100% }}
.timeline span {{ width:100%; background:var(--accent); border-radius:3px 3px 0 0; min-height:2px }}
.timeline label {{ font-size:10px; color:var(--dim); margin-top:6px; white-space:nowrap }}
.empty {{ color:var(--dim); margin:0 }}
.pill {{ background:#232733; border-radius:999px; padding:3px 10px; font-size:12px; color:var(--dim) }}
</style></head><body>
<header>
  <h1>Wikipedia Realtime Analytics</h1>
  <span class="pill">topic {_esc(TOPIC)}</span>
  <span class="pill">{state['total']} bản ghi</span>
  <span class="pill">window mới nhất {_hhmm(state['last_window'])} UTC</span>
  <span class="pill">cập nhật {_esc(now)} · tự refresh {REFRESH_S}s</span>
</header>
<div class="grid">
  <div class="card wide"><h2>Lượt sửa theo window 1 phút (tổng mọi wiki)</h2>{timeline_block}</div>
  <div class="card"><h2>edit_count · window 1 phút mới nhất theo wiki</h2>{edit_block}</div>
  <div class="card"><h2>bot_ratio · window 5 phút mới nhất</h2>{bot_block}</div>
  <div class="card wide"><h2>activity_spike · window vượt ngưỡng gần đây</h2>{spike_block}</div>
</div>
</body></html>"""


def consume_forever(state: dict, lock: threading.Lock) -> None:
    from confluent_kafka import Consumer  # chỉ cần khi chạy thật, nên selfcheck không phải cài

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([TOPIC])
    print(f"[dashboard] đọc {TOPIC} tại {BOOTSTRAP}, group {GROUP_ID}", flush=True)
    while True:
        message = consumer.poll(1.0)
        if message is None:
            continue
        if message.error():
            print(f"[dashboard] lỗi Kafka: {message.error()}", flush=True)
            continue
        try:
            record = json.loads(message.value())
        except (ValueError, TypeError):
            with lock:
                state["ignored"] += 1
            continue
        with lock:
            update_state(state, record)


def consume_raw_forever(state: dict, lock: threading.Lock) -> None:
    """Đọc topic raw, lọc ngay tại đây (~36 event/s vào, 2-3 giữ lại) rồi đẩy vào log."""
    from confluent_kafka import Consumer  # import muộn như consume_forever

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP_ID + "-raw",
        "auto.offset.reset": "latest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([RAW_TOPIC])
    print(f"[dashboard] đọc {RAW_TOPIC} cho log, wiki {sorted(LOG_WIKIS)}", flush=True)
    while True:
        message = consumer.poll(1.0)
        if message is None:
            continue
        if message.error():
            print(f"[dashboard] lỗi Kafka (raw): {message.error()}", flush=True)
            continue
        try:  # phong bì rồi tới raw_value (chuỗi JSON); lỗi ở bước nào cũng chỉ bỏ message đó
            entry = log_entry(json.loads(json.loads(message.value())["raw_value"]), LOG_WIKIS, LOG_KINDS)
        except (ValueError, TypeError, KeyError, AttributeError):
            continue
        if entry is not None:
            with lock:
                update_log(state, entry)


def main() -> None:
    state = new_state()
    lock = threading.Lock()
    threading.Thread(target=consume_forever, args=(state, lock), daemon=True).start()
    threading.Thread(target=consume_raw_forever, args=(state, lock), daemon=True).start()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            now = datetime.now(timezone.utc).strftime("%H:%M:%S")
            api = self.path.split("?", 1)[0] == "/api/state"
            # Snapshot dựng trong lock (consumer đang ghi các dict này), serialize ngoài lock.
            with lock:
                page = state_json(state, now) if api else render_page(state, now)
            body = (json.dumps(page) if api else page).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json" if api else "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_args):  # tắt log mỗi lần refresh
            pass

    print(f"[dashboard] http://localhost:{PORT}", flush=True)
    HTTPServer(("0.0.0.0", PORT), Handler).serve_forever()


if __name__ == "__main__":
    main()
