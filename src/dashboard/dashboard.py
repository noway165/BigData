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
# Dữ liệu (window_start/window_end, last_window, cutoff, emit_lag_seconds) luôn là UTC và không
# bao giờ bị dịch: mọi so sánh thứ tự và số học đều dựa vào đó. Offset này chỉ dùng lúc hiển thị,
# và client nhận nó từ server để hai đường render không lệch nhau.
DISPLAY_UTC_OFFSET = int(os.environ.get("DISPLAY_UTC_OFFSET", "7"))
DISPLAY_TZ_LABEL = "giờ VN"
SPIKE_FEED = 12            # số spike mới nhất hiện trên trang
SPIKE_COUNT_WINDOWS = 120  # số window giữ tally spike; window spike trượt 30 giây nên đủ cho 1 giờ
METRICS = ("edit_count", "bot_ratio", "activity_spike")


def new_state() -> dict:
    return {
        "edit_count": {},      # wiki -> bản ghi của window mới nhất
        "timeline": {},        # window_start -> tổng lượt sửa mọi wiki
        "bot_ratio": None,     # bản ghi window mới nhất
        "wiki_split": {},      # wiki -> edit_count/bot_edits/human_edits của window mới nhất
        "bot_history": {},     # window_start -> bản ghi bot_ratio mới nhất của window đó
        "log": deque(maxlen=LOG_LINES),  # dòng log, mới nhất đứng đầu
        "spikes": deque(maxlen=200),  # bản ghi spike cho feed; feed chỉ hiện SPIKE_FEED cái mới nhất
        # KPI recent_spikes đếm ở đây chứ không đếm deque trên: dòng thật ~30 spike mỗi 30 giây,
        # nên mọi cap đặt cho feed đều nhỏ hơn con số thật và KPI sẽ đi báo chính cái cap.
        "spike_counts": {},    # window_start -> số bản ghi activity_spike của window đó
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
        counts = state["spike_counts"]
        counts[window_start] = counts.get(window_start, 0) + 1
        _trim(counts, SPIKE_COUNT_WINDOWS)
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
            # Chưa có window timeline nào thì chưa có mốc "gần đây"; cutoff rỗng sẽ gom sạch
            # mọi window đã tally, nên vài giây đầu sau khởi động lại KPI sẽ phóng đại.
            "recent_spikes": sum(n for ws, n in state["spike_counts"].items() if ws >= cutoff) if timeline else 0,
            "emit_lag_seconds": lag,
            "total_records": state["total"],
        },
        "timeline": [{"window_start": ws, "edit_count": v} for ws, v in timeline],
        "wikis": [{"wiki": w, **v} for w, v in wikis],
        "bot_history": [dict(r) for _, r in sorted(state["bot_history"].items())],
        "spikes": [dict(r) for r in list(state["spikes"])[::-1][:SPIKE_FEED]],
        "log": [dict(e) for e in state["log"]],
        "status": {"now": now, "last_window": state["last_window"], "ignored": state["ignored"]},
    }


def _local(clock: str) -> str:
    """'18:30' hoặc '18:30:00' theo UTC -> cùng định dạng theo giờ hiển thị.

    Chỉ cộng vào phần giờ rồi lấy dư 24, nên 18:30 + 7 ra 01:30 chứ không phải 25:30. Chuỗi
    không phải giờ thì trả nguyên văn để nhánh escape ở trên vẫn xử lý được.
    """
    parts = clock.split(":")
    if len(parts) < 2 or not parts[0].isdigit():
        return clock
    return ":".join([f"{(int(parts[0]) + DISPLAY_UTC_OFFSET) % 24:02d}"] + parts[1:])


def _hhmm(iso: str) -> str:
    """'2026-11-07T18:48:00.000Z' (UTC) -> '01:48' theo giờ hiển thị. Chuỗi lạ thì trả nguyên văn.

    Escape luôn: window_start đến từ Kafka và mọi chỗ gọi hàm này đều nhúng thẳng vào HTML.
    """
    if not iso or "T" not in iso:
        return _esc(iso) if iso else "—"
    return _esc(_local(iso.split("T", 1)[1][:5]))


def _bar(ratio: float, color: str) -> str:
    width = max(0.0, min(1.0, ratio)) * 100
    return f'<div class="bar"><span style="width:{width:.1f}%;background:{color}"></span></div>'


def _esc(value) -> str:
    return html.escape(str(value), quote=True)


def _noscript_blocks(state: dict) -> str:
    """Các khối render sẵn ở server cho `<noscript>`: trang vẫn có số liệu nếu JavaScript chết."""
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
                     f'–{_hhmm(bot.get("window_end") or "")} {DISPLAY_TZ_LABEL} · {total} event</p>')
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

    return f"""<div class="grid">
  <div class="card wide"><h2>Lượt sửa theo window 1 phút (tổng mọi wiki)</h2>{timeline_block}</div>
  <div class="card"><h2>edit_count · window 1 phút mới nhất theo wiki</h2>{edit_block}</div>
  <div class="card"><h2>bot_ratio · window 5 phút mới nhất</h2>{bot_block}</div>
  <div class="card wide"><h2>activity_spike · window vượt ngưỡng gần đây</h2>{spike_block}</div>
</div>"""


# Bảng màu lấy từ skill dataviz và đã chạy qua validate_palette.js ở cả hai chế độ:
# hai slot phân loại dùng cạnh nhau (xanh = người, cam = bot) đạt mọi ngưỡng CVD và tương phản.
# Luôn nền sáng, kể cả khi máy đặt chế độ tối: trang này chiếu lên tường lớp học.
_CSS = """
:root{
  --bg:#f9f9f7; --card:#fcfcfb; --line:#e1e0d9; --axis:#c3c2b7; --track:#eceae4;
  /* --muted đi vào chữ nhãn trục cỡ nhỏ, nên phải đạt AA: #898781 chỉ được 3.5:1 trên nền sáng */
  --fg:#0b0b0b; --dim:#52514e; --muted:#6b6a65;
  --accent:#2a78d6; --human:#2a78d6; --bot:#eb6834; --unknown:#898781;
  --good:#0ca30c; --warn:#a66300; --crit:#d03b3b; --up:#006300; --down:#c23434;
}
*{box-sizing:border-box}
body{margin:0;padding:clamp(12px,1.5vw,28px);background:var(--bg);color:var(--fg);
     font:1rem/1.5 system-ui,-apple-system,"Segoe UI",Roboto,sans-serif}
header{display:flex;align-items:baseline;gap:.6rem 1rem;flex-wrap:wrap;margin-bottom:1rem}
h1{font-size:clamp(1.3rem,2.1vw,1.9rem);margin:0;letter-spacing:-.015em}
h2{font-size:.875rem;margin:0 0 .8rem;color:var(--dim);font-weight:600;
   text-transform:uppercase;letter-spacing:.07em}
.pill{background:var(--track);border-radius:999px;padding:.2rem .7rem;font-size:.8125rem;
      color:var(--dim);white-space:nowrap}
/* Lưới 12 cột với span tường minh: auto-fit trên máy chiếu 1920px đẻ ra 5 cột hẹp.
   min-width:0 là bắt buộc, nếu không một bảng rộng sẽ nong cả trang ra và đẩy cột cuối ra ngoài. */
.grid{display:grid;gap:1rem;grid-template-columns:repeat(12,1fr)}
.card{min-width:0;grid-column:span 12;background:var(--card);border:1px solid var(--line);
      border-radius:12px;padding:1rem 1.15rem}
.card.half{grid-column:span 6}
.empty{color:var(--dim);margin:0}

/* KPI: con số là bằng chứng chính, phải đọc được từ cuối lớp */
.kpi{display:grid;gap:1rem;grid-template-columns:repeat(auto-fit,minmax(10.5rem,1fr));margin-bottom:1rem}
.tile{background:var(--card);border:1px solid var(--line);border-radius:12px;padding:.85rem 1.1rem}
.tile .v{display:block;font-size:clamp(2rem,4vw,3.25rem);line-height:1.04;font-weight:600;
         letter-spacing:-.02em;font-variant-numeric:tabular-nums}
.tile .l{display:block;margin-top:.3rem;font-size:.8125rem;color:var(--dim);
         text-transform:uppercase;letter-spacing:.06em}

svg{display:block;width:100%;height:auto}
.tick{fill:var(--muted);font:15px system-ui,sans-serif}
.val{fill:var(--fg);font:600 19px system-ui,sans-serif}

table{width:100%;border-collapse:collapse}
#tbl-spikes{table-layout:fixed}  /* cố định cột để tiêu đề trang dài bị cắt bằng ellipsis */
td,th{padding:.35rem .5rem;border-bottom:1px solid var(--line);text-align:left;vertical-align:middle}
th{color:var(--dim);font-weight:600;font-size:.8125rem;text-transform:uppercase;letter-spacing:.05em}
.k{font-family:ui-monospace,Consolas,monospace;white-space:nowrap;color:var(--dim)}
.n,th.n{text-align:right;white-space:nowrap;font-variant-numeric:tabular-nums}
.dim{color:var(--dim)}
.tt{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.legend{list-style:none;display:flex;gap:1.1rem;padding:0;margin:0 0 .8rem;flex-wrap:wrap;
        font-size:.875rem;color:var(--dim)}
.legend i{display:inline-block;width:.7rem;height:.7rem;border-radius:3px;margin-right:.4rem}

/* Top wiki: một thanh chia hai phần người/bot, dài theo wiki đông nhất */
.split-row{display:grid;grid-template-columns:9rem 1fr 5rem 6.5rem;gap:.8rem;align-items:center;
           padding:.33rem 0;border-bottom:1px solid var(--line)}
.split{display:flex;height:.9rem;background:var(--track);border-radius:4px;overflow:hidden}
.split span{display:block;height:100%}
.split span+span{margin-left:2px}
.seg-human{background:var(--human)}
.seg-bot{background:var(--bot)}

/* Log trực tiếp: phần làm buổi demo có nhịp sống */
.log-row{display:grid;grid-template-columns:5rem 7rem 8rem 8.5rem minmax(0,1fr) 5.5rem;gap:.8rem;
         align-items:baseline;padding:.33rem 0;border-bottom:1px solid var(--line);font-size:1.0625rem}
.log-row .t{font-family:ui-monospace,Consolas,monospace;color:var(--dim);font-variant-numeric:tabular-nums}
.log-row .kind{color:var(--dim);font-size:.9375rem}
.badge{margin-left:.4rem;border:1px solid var(--bot);color:var(--bot);border-radius:4px;
       padding:0 .3rem;font-size:.75rem;font-weight:600;letter-spacing:.03em}
.up{color:var(--up)}
.down{color:var(--down)}

/* Thanh trạng thái pipeline: mỗi chặng một ký hiệu + nhãn, không bao giờ chỉ dựa vào màu */
.statusbar{display:grid;gap:.9rem;grid-template-columns:repeat(auto-fit,minmax(13rem,1fr));
           background:var(--card);border:1px solid var(--line);border-radius:12px;
           padding:1rem 1.15rem;margin-top:1rem}
.stage{display:grid;grid-template-columns:1.1rem 1fr;gap:.45rem;align-items:start}
.stage .dot{color:var(--muted);font-size:.95rem;line-height:1.5}
.stage[data-state=good] .dot{color:var(--good)}
.stage[data-state=warn] .dot{color:var(--warn)}
.stage[data-state=crit] .dot{color:var(--crit)}
.stage .name{font-weight:600}
.stage .sval{display:block;color:var(--dim);font-size:.875rem}

/* Các khối render sẵn trong <noscript> dùng lại đúng các token trên */
.card.wide{grid-column:1/-1}
.chart{width:45%}
.bar{background:var(--track);border-radius:3px;height:9px;overflow:hidden}
.bar span{display:block;height:100%}
.stack{display:flex;height:22px;border-radius:5px;overflow:hidden;background:var(--track)}
.stack span{display:block}
.timeline{display:flex;align-items:flex-end;gap:4px;height:140px}
.timeline .col{flex:1;display:flex;flex-direction:column;justify-content:flex-end;
               align-items:center;height:100%}
.timeline span{width:100%;background:var(--accent);border-radius:3px 3px 0 0;min-height:2px}
.timeline label{font-size:10px;color:var(--dim);margin-top:6px;white-space:nowrap}

@media (max-width:900px){
  .card.half{grid-column:span 12}
}
@media (max-width:700px){
  .log-row{grid-template-columns:4.5rem minmax(0,1fr) 4.5rem}
  .split-row{grid-template-columns:7rem 1fr 4.5rem}
  .split-row :nth-child(4){display:none}
  .hide-s{display:none}
}
"""

# Client: poll /api/state mỗi 2 giây rồi vá DOM tại chỗ. Mọi chữ đi qua textContent, nên tiêu đề
# trang Wikipedia chứa '<' hay '&' cũng không thể thành markup. Không reload, nên trang không
# nhảy về đầu và không nháy.
_JS = """
var NS='http://www.w3.org/2000/svg';
var KIND={edit:'Chỉnh sửa bài',"new":'Thêm bài mới'};
var GLYPH={good:'\\u25cf',warn:'\\u25b2',crit:'\\u2716'};
function g(id){return document.getElementById(id)}
function num(v){return v==null?'\\u2014':Number(v).toLocaleString('vi-VN')}
function pct(v){return v==null?'\\u2014':(v*100).toFixed(1)+'%'}
// TZ_OFFSET và TZ_LABEL do server nhúng vào trang. Cố ý KHÔNG dùng múi giờ của trình duyệt:
// container chạy theo giờ gốc (offset 0), nên đường dựng sẵn ở server và đường client sẽ lệch nhau.
function local(clock){
  var p=String(clock==null?'':clock).split(':');
  if(p.length<2||!/^\\d+$/.test(p[0]))return clock;
  p[0]=('0'+((Number(p[0])+TZ_OFFSET)%24)).slice(-2);
  return p.join(':');
}
function hhmm(iso){var i=(iso||'').indexOf('T');return i<0?(iso||'\\u2014'):local(iso.slice(i+1,i+6))}
function el(tag,cls,text){var e=document.createElement(tag);
  if(cls)e.className=cls; if(text!=null)e.textContent=text; return e}
function sv(tag,attrs){var e=document.createElementNS(NS,tag);
  for(var k in attrs)e.setAttribute(k,attrs[k]); return e}
function tip(mark,text){var t=document.createElementNS(NS,'title');
  t.textContent=text; mark.appendChild(t); return mark}

function kpiRow(k){
  g('kpi-epm').textContent=num(k.edits_per_minute);
  g('kpi-bot').textContent=pct(k.bot_share);
  g('kpi-wikis').textContent=num(k.active_wikis);
  g('kpi-spikes').textContent=num(k.recent_spikes);
  g('kpi-lag').textContent=k.emit_lag_seconds==null?'\\u2014':Math.round(k.emit_lag_seconds)+'s';
  g('kpi-total').textContent=num(k.total_records);
}

// Cột lượt sửa/phút: một chuỗi số liệu nên không cần legend, tiêu đề đã nói nó là gì.
function editsChart(rows){
  var box=g('chart-edits');
  if(!rows.length){box.replaceChildren(el('p','empty','Chưa có dữ liệu theo thời gian.')); return}
  var W=1000,H=260,L=64,R=16,T=20,B=34,iw=W-L-R,ih=H-T-B;
  var peak=Math.max.apply(null,rows.map(function(r){return r.edit_count||0}))||1;
  var bw=iw/rows.length;
  var svg=sv('svg',{viewBox:'0 0 '+W+' '+H,role:'img',
                    'aria-label':'Lượt sửa theo từng window 1 phút, '+rows.length+' window'});
  [0,0.5,1].forEach(function(f){
    var y=T+ih-ih*f;
    svg.appendChild(sv('line',{x1:L,x2:W-R,y1:y,y2:y,'stroke-width':1,
                               stroke:f?'var(--line)':'var(--axis)'}));
    var t=sv('text',{x:L-10,y:y+5,'text-anchor':'end','class':'tick'});
    t.textContent=num(Math.round(peak*f)); svg.appendChild(t);
  });
  rows.forEach(function(r,i){
    var h=Math.max(2,(r.edit_count||0)/peak*ih);
    var bar=sv('rect',{x:(L+i*bw+1).toFixed(1),y:(T+ih-h).toFixed(1),rx:3,
                       width:Math.max(1,bw-2).toFixed(1),height:h.toFixed(1),fill:'var(--accent)'});
    svg.appendChild(tip(bar,hhmm(r.window_start)+' '+TZ_LABEL+' · '+num(r.edit_count)+' lượt sửa'));
    if(i===rows.length-1||i%4===0){
      var t=sv('text',{x:(L+i*bw+bw/2).toFixed(1),y:H-12,'text-anchor':'middle','class':'tick'});
      t.textContent=hhmm(r.window_start); svg.appendChild(t);
    }
  });
  var last=rows[rows.length-1], lh=Math.max(2,(last.edit_count||0)/peak*ih);
  var lab=sv('text',{x:(L+iw-bw/2).toFixed(1),y:(T+ih-lh-9).toFixed(1),
                     'text-anchor':'middle','class':'val'});
  lab.textContent=num(last.edit_count); svg.appendChild(lab);
  box.replaceChildren(svg);
}

// Top wiki: thanh chia người/bot, thêm hai cột số nên danh tính không chỉ nằm ở màu.
function wikiSplit(rows){
  var box=g('list-wikis');
  if(!rows.length){box.replaceChildren(el('p','empty','Chưa có window nào đóng.')); return}
  var peak=Math.max.apply(null,rows.map(function(r){return r.edit_count||0}))||1;
  var frag=document.createDocumentFragment();
  rows.forEach(function(r){
    var row=el('div','split-row');
    row.appendChild(el('span','k',r.wiki));
    var track=el('div','split');
    [['seg-human',r.human_edits,'Người'],['seg-bot',r.bot_edits,'Bot']].forEach(function(p){
      var seg=el('span',p[0]);
      seg.style.width=((p[1]||0)/peak*100).toFixed(2)+'%';
      seg.title=p[2]+': '+num(p[1]);
      track.appendChild(seg);
    });
    row.appendChild(track);
    row.appendChild(el('span','n',num(r.edit_count)));
    row.appendChild(el('span','n dim',num(r.bot_edits)+' bot'));
    frag.appendChild(row);
  });
  box.replaceChildren(frag);
}

// Đường tỉ lệ bot: trục y cố định 0-100% để mắt so được giữa các lần cập nhật.
function botTrend(rows){
  var box=g('chart-bot');
  if(!rows.length){box.replaceChildren(el('p','empty',
      'Chưa có window bot_ratio nào đóng (tumbling 5 phút, nên chờ lâu nhất).')); return}
  // viewBox rộng ~một nửa biểu đồ cột vì thẻ này chiếm nửa lưới: cùng tỉ lệ đơn vị/pixel thì
  // chữ của hai biểu đồ hiện ra cùng một cỡ thật.
  var W=480,H=190,L=46,R=16,T=14,B=26,iw=W-L-R,ih=H-T-B;
  var svg=sv('svg',{viewBox:'0 0 '+W+' '+H,role:'img',
                    'aria-label':'Tỉ lệ bot theo window 5 phút'});
  [0,0.25,0.5,0.75,1].forEach(function(f){
    var y=T+ih-ih*f;
    svg.appendChild(sv('line',{x1:L,x2:W-R,y1:y,y2:y,'stroke-width':0.5,
                               stroke:f?'var(--line)':'var(--axis)'}));
    var t=sv('text',{x:L-7,y:y+5,'text-anchor':'end','class':'tick'});
    t.textContent=(f*100)+'%'; svg.appendChild(t);
  });
  var px=function(i){return rows.length>1?L+iw*i/(rows.length-1):L+iw/2};
  var py=function(v){return T+ih-ih*Math.max(0,Math.min(1,v||0))};
  svg.appendChild(sv('polyline',{fill:'none',stroke:'var(--bot)','stroke-width':2,
    'stroke-linejoin':'round','stroke-linecap':'round',
    points:rows.map(function(r,i){return px(i).toFixed(1)+','+py(r.bot_share).toFixed(1)}).join(' ')}));
  rows.forEach(function(r,i){
    var c=sv('circle',{cx:px(i).toFixed(1),cy:py(r.bot_share).toFixed(1),r:4,
                       fill:'var(--bot)',stroke:'var(--card)','stroke-width':1.5});
    svg.appendChild(tip(c,hhmm(r.window_start)+'\\u2013'+hhmm(r.window_end)+' '+TZ_LABEL+' · bot '
                        +pct(r.bot_share)+' / '+num(r.total_events)+' event'));
    if(i===rows.length-1||i%3===0){
      var t=sv('text',{x:px(i).toFixed(1),y:H-8,'text-anchor':'middle','class':'tick'});
      t.textContent=hhmm(r.window_start); svg.appendChild(t);
    }
  });
  var last=rows[rows.length-1];
  var lab=sv('text',{x:px(rows.length-1).toFixed(1),y:(py(last.bot_share)-11).toFixed(1),
                     'text-anchor':'end','class':'val'});
  lab.textContent=pct(last.bot_share); svg.appendChild(lab);
  box.replaceChildren(svg);
}

function spikeFeed(rows){
  var frag=document.createDocumentFragment();
  if(!rows.length){
    var tr=document.createElement('tr'), td=el('td','empty','Chưa có window nào vượt ngưỡng.');
    td.colSpan=6; tr.appendChild(td); frag.appendChild(tr);
  }
  rows.forEach(function(r){
    var tr=document.createElement('tr');
    [el('td','k',r.wiki),el('td','tt',r.title||'\\u2014'),el('td','n',num(r.edit_count)),
     el('td','n dim',num(r.distinct_users)+' người'),el('td','n dim','>'+num(r.threshold_edits)),
     el('td','n dim',hhmm(r.window_start))].forEach(function(td){tr.appendChild(td)});
    frag.appendChild(tr);
  });
  g('rows-spikes').replaceChildren(frag);
}

function liveLog(rows){
  var frag=document.createDocumentFragment();
  if(!rows.length)frag.appendChild(el('p','empty','Chưa có event nào qua bộ lọc wiki.'));
  rows.slice(0,14).forEach(function(e){
    var row=el('div','log-row');
    row.appendChild(el('span','t',local(e.time)));
    row.appendChild(el('span','k',e.wiki));
    var who=el('span','hide-s');
    who.appendChild(el('span',null,e.user));
    if(e.bot)who.appendChild(el('span','badge','Bot'));
    row.appendChild(who);
    row.appendChild(el('span','kind hide-s',KIND[e.kind]||e.kind));
    row.appendChild(el('span','tt',e.title));
    var cls=e.delta>0?'n up':e.delta<0?'n down':'n dim';
    row.appendChild(el('span',cls,e.delta==null?'\\u2014':(e.delta>0?'+':'')+num(e.delta)+' B'));
    frag.appendChild(row);
  });
  g('rows-log').replaceChildren(frag);
}

// 'HH:MM:SS' -> khoảng cách giây; gói qua nửa đêm để không ra số âm khổng lồ.
function gap(a,b){
  function parse(s){var p=(s||'').split(':');
    return p.length===3?(+p[0])*3600+(+p[1])*60+(+p[2]):null}
  var x=parse(a), y=parse(b);
  if(x==null||y==null||isNaN(x)||isNaN(y))return null;
  var d=y-x; return d<-43200?d+86400:d;
}
function stage(id,state,text){
  var e=g(id);
  e.dataset.state=state;
  e.querySelector('.dot').textContent=GLYPH[state];
  e.querySelector('.sval').textContent=text;
}
function statusBar(d,online){
  var st=d.status, k=d.kpi, age=d.log.length?gap(d.log[0].time,st.now):null;
  stage('st-source',online&&age!=null&&age<=30?'good':'warn',
        d.log.length?'event mới nhất '+local(d.log[0].time)+' '+TZ_LABEL:'chưa nhận event nào');
  stage('st-raw',d.log.length?'good':'warn',num(d.log.length)+' dòng trong bộ đệm log');
  stage('st-spark',k.emit_lag_seconds==null?'warn':k.emit_lag_seconds<=120?'good':'warn',
        k.emit_lag_seconds==null?'chưa có window nào đóng'
                                :'trễ phát '+Math.round(k.emit_lag_seconds)+'s');
  stage('st-analytics',k.total_records?'good':'warn',
        num(k.total_records)+' bản ghi · bỏ '+num(st.ignored));
  stage('st-page',online?'good':'crit',
        online?'cập nhật '+local(st.now)+' '+TZ_LABEL:'mất kết nối /api/state');
}

function headerPills(d){
  g('pill-total').textContent=num(d.kpi.total_records)+' bản ghi';
  g('pill-window').textContent='window mới nhất '+hhmm(d.status.last_window)+' '+TZ_LABEL;
}

// Mỗi khối tự chịu lỗi của mình: một bản ghi dị dạng trong KPI không được kéo theo cả log và
// thanh trạng thái. Nhờ vậy render() không bao giờ ném, nên lỗi vẽ không bị báo nhầm là mất mạng.
function safe(name,fn,arg){
  try{ fn(arg) }catch(err){ console.error('[dashboard] khối '+name+' lỗi:',err) }
}
function render(d){
  safe('KPI',kpiRow,d.kpi);
  safe('lượt sửa',editsChart,d.timeline);
  safe('top wiki',wikiSplit,d.wikis);
  safe('tỉ lệ bot',botTrend,d.bot_history);
  safe('spike',spikeFeed,d.spikes);
  safe('log',liveLog,d.log);
  safe('trạng thái',function(x){statusBar(x,true)},d);
  safe('header',headerPills,d);
}

var latest=null, busy=false;
async function tick(){
  if(busy)return;  // một poll chậm trả về sau poll mới hơn sẽ vẽ đè dữ liệu cũ lên dữ liệu mới
  busy=true;
  // Phải có timeout: một kết nối treo nửa chừng (hay gặp sau khi container khởi động lại) sẽ giữ
  // cờ busy mãi mãi, trang đứng im mà chặng 5 vẫn xanh — kiểu hỏng tệ nhất trên màn chiếu.
  // abort làm fetch ném, nên nó đi đúng nhánh catch như mọi lỗi mạng khác.
  var stop=new AbortController();
  var timer=setTimeout(function(){stop.abort()},5000);
  try{
    var res=await fetch('/api/state',{cache:'no-store',signal:stop.signal});
    if(!res.ok)throw new Error('HTTP '+res.status);
    var data=await res.json();
    latest=data;
    render(data);
  }catch(err){
    // Chỉ tới được đây khi fetch/parse hỏng. Giữ nguyên số liệu cũ trên màn hình và đổi chặng
    // cuối sang đỏ: màn chiếu không được trắng giữa buổi bảo vệ.
    console.error('[dashboard] không lấy được /api/state:',err);
    if(latest)safe('trạng thái',function(x){statusBar(x,false)},latest);
  }finally{
    clearTimeout(timer);
    busy=false;
  }
}
tick();
setInterval(tick,2000);
"""

_STAGES = (("st-source", "1 · Wikimedia SSE", "đang chờ"),
           ("st-raw", "2 · Kafka topic raw", "đang chờ"),
           ("st-spark", "3 · Spark Structured Streaming", "đang chờ"),
           ("st-analytics", "4 · Kafka topic analytics", "đang chờ"),
           ("st-page", "5 · Trang này", "đang chờ"))

_KPIS = (("kpi-epm", "Lượt sửa / phút"), ("kpi-bot", "Tỉ lệ bot"), ("kpi-wikis", "Wiki hoạt động"),
         ("kpi-spikes", "Spike gần đây"), ("kpi-lag", "Trễ phát"), ("kpi-total", "Bản ghi đã nạp"))


def render_page(state: dict, now: str) -> str:
    """Khung trang + CSS + script inline + `<noscript>`. Không tải bất kỳ tài nguyên ngoài nào."""
    tiles = "".join(f'<div class="tile"><span class="v" id="{i}">…</span>'
                    f'<span class="l">{label}</span></div>' for i, label in _KPIS)
    stages = "".join(f'<div class="stage" id="{i}"><span class="dot">·</span>'
                     f'<span><span class="name">{name}</span>'
                     f'<span class="sval">{value}</span></span></div>' for i, name, value in _STAGES)
    return f"""<!doctype html>
<html lang="vi"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<noscript><meta http-equiv="refresh" content="{REFRESH_S}">
<style>#live,.kpi,.statusbar{{display:none}}</style></noscript>
<title>Wikipedia Realtime Analytics</title>
<style>{_CSS}</style></head><body>
<header>
  <h1>Wikipedia Realtime Analytics</h1>
  <span class="pill">topic {_esc(TOPIC)}</span>
  <span class="pill" id="pill-total">{state['total']} bản ghi</span>
  <span class="pill" id="pill-window">window mới nhất {_hhmm(state['last_window'])} {DISPLAY_TZ_LABEL}</span>
  <span class="pill">tự cập nhật 2s · {_esc(_local(now))} {DISPLAY_TZ_LABEL}</span>
</header>
<div class="kpi">{tiles}</div>
<div class="grid" id="live">
  <div class="card"><h2>Lượt sửa mỗi phút · {TIMELINE_WINDOWS} window gần nhất</h2>
    <div id="chart-edits"><p class="empty">Đang tải…</p></div></div>
  <div class="card half"><h2>Top wiki · window 1 phút mới nhất</h2>
    <ul class="legend"><li><i style="background:var(--human)"></i>Người sửa</li>
      <li><i style="background:var(--bot)"></i>Bot</li></ul>
    <div id="list-wikis"><p class="empty">Đang tải…</p></div></div>
  <div class="card half"><h2>Tỉ lệ bot theo window 5 phút</h2>
    <div id="chart-bot"><p class="empty">Đang tải…</p></div></div>
  <div class="card"><h2>activity_spike · window vượt ngưỡng gần đây</h2>
    <table id="tbl-spikes"><colgroup><col style="width:10rem"><col><col style="width:7rem">
      <col style="width:8rem"><col style="width:6rem"><col style="width:7rem"></colgroup>
      <thead><tr><th>Wiki</th><th>Trang</th><th class="n">Lượt sửa</th>
      <th class="n">Người sửa</th><th class="n">Ngưỡng</th><th class="n">Window</th></tr></thead>
      <tbody id="rows-spikes"><tr><td class="empty" colspan="6">Đang tải…</td></tr></tbody></table></div>
  <div class="card"><h2>Log trực tiếp · {_esc(", ".join(sorted(LOG_WIKIS)))}</h2>
    <div id="rows-log"><p class="empty">Đang tải…</p></div></div>
</div>
<div class="statusbar">{stages}</div>
<noscript>
  <p class="empty">JavaScript đang tắt: trang tự refresh {REFRESH_S}s và chỉ hiện các khối dựng ở server.</p>
  {_noscript_blocks(state)}
</noscript>
<script>var TZ_OFFSET={DISPLAY_UTC_OFFSET},TZ_LABEL={json.dumps(DISPLAY_TZ_LABEL)};{_JS}</script>
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
        # RecursionError nằm trong danh sách vì json.loads nổ nó với JSON lồng quá sâu; để sót thì
        # thread daemon này chết im lặng và log đứng luôn.
        except (ValueError, TypeError, KeyError, AttributeError, RecursionError):
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
            if api:  # client poll 2 giây một lần, không được để browser trả lại bản cache
                self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_args):  # tắt log mỗi lần refresh
            pass

    print(f"[dashboard] http://localhost:{PORT}", flush=True)
    HTTPServer(("0.0.0.0", PORT), Handler).serve_forever()


if __name__ == "__main__":
    main()
