# Dashboard UI Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rebuild the dashboard page so it updates without reloading, shows six information blocks instead of three, and carries a live feed of individual Wikipedia edits.

**Architecture:** Keep the single-file server. Add a second Kafka consumer thread for the raw topic, expose the whole view model at `/api/state` as JSON, and let the browser fetch it every 2 seconds and patch the DOM. Charts are hand-written SVG. The current server-rendered page stays as the `<noscript>` fallback.

**Tech Stack:** Python 3.12 standard library (`http.server`, `threading`, `json`, `html`), `confluent-kafka` (already in the producer image), vanilla JavaScript, inline SVG and CSS.

**Spec:** [2026-11-09-dashboard-ui-design.md](../specs/2026-11-09-dashboard-ui-design.md)

## Global Constraints

- One file: `src/dashboard/dashboard.py`. No new Python dependency, no new image, no new compose service.
- The page loads no external resource. Everything is inline.
- `tests/dashboard_selfcheck.py` must keep running on a host without `confluent-kafka`: import it lazily, inside the consumer functions only.
- Analytics consumer: topic `wikipedia-analytics`, `auto.offset.reset=earliest`, fresh `group.id` per start.
- Raw consumer: topic `wikipedia-recent-changes`, `auto.offset.reset=latest`, its own fresh `group.id`.
- Log shows only `enwiki` and `viwiki`, only `type` in `edit` and `new`, keeps the 60 newest lines, newest first.
- Usernames are masked. The raw `user` value never reaches the page or `/api/state`.
- The `comment` field is never sent to the page.
- Comments in the code are Vietnamese, matching the rest of the repo.

## Review Focus

1. **`user` missing or `null`** — a log line must still render, as `ẩn danh`, not crash the consumer thread. Covered in Task 1.
2. **`length.old` missing on a new page** — delta is `+length.new`, not a `TypeError`. Covered in Task 1.
3. **A page title containing `<` or `&`** — must appear as text in both the `<noscript>` render and the client render, never as markup. Covered in Task 1 and Task 4.
4. **`meta.dt` missing** — the entry is dropped rather than rendering an empty time cell. Covered in Task 1.
5. **`/api/state` read while a consumer thread writes** — the handler must serialize a snapshot under the lock; a `RuntimeError: dictionary changed size during iteration` in production would empty the page. Covered in Task 3.

---

### Task 1: Log entry parsing and user masking

**Files:**
- Modify: `src/dashboard/dashboard.py`
- Test: `tests/dashboard_selfcheck.py`

**Interfaces:**
- Consumes: nothing.
- Produces: `mask_user(name: str | None) -> str`, `log_entry(event: dict, wikis: frozenset[str], kinds: frozenset[str]) -> dict | None`. A returned entry has keys `time` (`"HH:MM:SS"`), `wiki`, `user` (masked), `bot` (bool), `kind` (`"edit"` or `"new"`), `title`, `delta` (`int | None`).

- [ ] **Step 1: Write the failing tests**

```python
assert mask_user("Printstream") == "Pri***"
assert mask_user("Ab") == "A***"
assert mask_user("171.224.3.9") == "171.224.*.*"
assert mask_user("2001:db8:85a3::8a2e") == "2001:db8:*"
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

assert log_entry(raw(wiki="commonswiki"), WIKIS, KINDS) is None
assert log_entry(raw(type="categorize"), WIKIS, KINDS) is None
assert log_entry(raw(type="log"), WIKIS, KINDS) is None
assert log_entry(raw(meta={}), WIKIS, KINDS) is None
assert log_entry(raw(user=None), WIKIS, KINDS)["user"] == "ẩn danh"
assert log_entry(raw(type="new", length={"new": 800}), WIKIS, KINDS)["delta"] == 800
assert log_entry(raw(length={}), WIKIS, KINDS)["delta"] is None
assert log_entry(raw(bot=True), WIKIS, KINDS)["bot"] is True
assert "comment" not in log_entry(raw(comment="xin chao"), WIKIS, KINDS)
assert log_entry(raw(title="A & <b>"), WIKIS, KINDS)["title"] == "A & <b>"
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `python tests/dashboard_selfcheck.py`
Expected: FAIL with `NameError: name 'mask_user' is not defined`

- [ ] **Step 3: Implement `mask_user` and `log_entry` in `src/dashboard/dashboard.py`**

`mask_user`: empty or `None` gives `"ẩn danh"`; four dot-separated numeric parts gives the first two plus `".*.*"`; a value containing `":"` gives the first two colon-groups plus `":*"`; anything else gives the first three characters plus `"***"`, and a name shorter than three characters keeps only its first character. `log_entry` returns `None` unless `wiki` is in `wikis`, `type` is in `kinds`, and `meta.dt` is present; `delta` is `length.new - length.old` with a missing `old` treated as `0` and a missing `new` giving `None`. The entry holds no field other than the seven named above — escaping belongs to the renderer, not here.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `python tests/dashboard_selfcheck.py`
Expected: `DASHBOARD SELFCHECK OK`

- [ ] **Step 5: Commit**

```bash
git add src/dashboard/dashboard.py tests/dashboard_selfcheck.py
git commit -m "Parse and mask raw events for the dashboard log"
```

---

### Task 2: Extended state

**Files:**
- Modify: `src/dashboard/dashboard.py`
- Test: `tests/dashboard_selfcheck.py`

**Interfaces:**
- Consumes: `log_entry` from Task 1.
- Produces: `new_state()` gains `wiki_split: dict[str, dict]` (per wiki, the newest window's `edit_count`, `bot_edits` and derived `human_edits`), `bot_history: dict[str, dict]` (window_start to the newest `bot_ratio` record for it, trimmed to `BOT_HISTORY_WINDOWS`), and `log: collections.deque` (newest first, `maxlen=LOG_LINES`). `update_log(state: dict, entry: dict) -> dict` pushes one parsed entry.

- [ ] **Step 1: Write the failing tests**

```python
state = new_state()
update_state(state, edit("enwiki", 11, 55, bots=20))
assert state["wiki_split"]["enwiki"] == {"edit_count": 55, "bot_edits": 20, "human_edits": 35,
                                         "window_start": "2026-11-08T10:11:00.000Z"}

update_state(state, ratio(10, 30, 60, 10))
update_state(state, ratio(15, 10, 80, 10))
assert sorted(state["bot_history"]) == ["2026-11-08T10:10:00.000Z", "2026-11-08T10:15:00.000Z"]

update_log(state, {"time": "16:00:01", "wiki": "enwiki", "user": "A***", "bot": False,
                   "kind": "edit", "title": "First", "delta": 10})
update_log(state, {"time": "16:00:02", "wiki": "viwiki", "user": "B***", "bot": True,
                   "kind": "new", "title": "Second", "delta": None})
assert [e["title"] for e in state["log"]] == ["Second", "First"]
assert state["log"].maxlen == 60
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `python tests/dashboard_selfcheck.py`
Expected: FAIL on the `wiki_split` assertion with `KeyError: 'wiki_split'`

- [ ] **Step 3: Extend `new_state` and `update_state`, and add `update_log`, in `src/dashboard/dashboard.py`**

`wiki_split` and `bot_history` follow the same out-of-order rule the existing code uses for `edit_count`: a record whose `window_start` is older than the one already stored is ignored. `bot_history` is trimmed with the existing `_trim`. `update_log` uses `appendleft`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `python tests/dashboard_selfcheck.py`
Expected: `DASHBOARD SELFCHECK OK`

- [ ] **Step 5: Commit**

```bash
git add src/dashboard/dashboard.py tests/dashboard_selfcheck.py
git commit -m "Keep per-wiki split, bot ratio history and the log in dashboard state"
```

---

### Task 3: JSON endpoint and the raw consumer

**Files:**
- Modify: `src/dashboard/dashboard.py`, `docker-compose.yml`, `.env.example`
- Test: `tests/dashboard_selfcheck.py`

**Interfaces:**
- Consumes: Task 2's state.
- Produces: `state_json(state: dict, now: str) -> dict` with keys `kpi`, `timeline`, `wikis`, `bot_history`, `spikes`, `log`, `status`; `consume_raw_forever(state: dict, lock: threading.Lock) -> None`. `GET /api/state` returns `state_json` as `application/json`; every other path returns the page.

- [ ] **Step 1: Write the failing tests**

```python
import json
payload = state_json(state, "12:00:00")
assert set(payload) == {"kpi", "timeline", "wikis", "bot_history", "spikes", "log", "status"}
assert json.dumps(payload)                      # phải serialize được, deque thì không
assert payload["kpi"]["total_records"] == state["total"]
assert payload["log"][0]["title"] == "Second"
assert all("comment" not in e for e in payload["log"])
assert payload["status"]["last_window"] == state["last_window"]
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `python tests/dashboard_selfcheck.py`
Expected: FAIL with `NameError: name 'state_json' is not defined`

- [ ] **Step 3: Implement `state_json`, `consume_raw_forever` and the `/api/state` route**

`state_json` converts the deque to a list and the window maps to time-sorted lists, so the result is JSON-serializable. `kpi` carries `edits_per_minute` (the newest timeline window's total), `bot_share`, `active_wikis`, `recent_spikes` (spikes inside the newest 10 windows), `emit_lag_seconds` (`emitted_at` minus `window_end` of the newest analytics record) and `total_records`. `consume_raw_forever` mirrors `consume_forever`: it imports `confluent_kafka` inside the function, subscribes to `RAW_TOPIC` with `auto.offset.reset=latest`, and for each message parses the envelope's `raw_value`, calls `log_entry`, and calls `update_log` under the lock when it is not `None`. `main` starts it as a second daemon thread. The request handler builds the snapshot inside `with lock:` and serializes it outside.

New module constants read from the environment: `RAW_TOPIC` (`KAFKA_TOPIC`, default `wikipedia-recent-changes`), `LOG_WIKIS` (`DASHBOARD_LOG_WIKIS`, default `enwiki,viwiki`), `LOG_KINDS` (fixed `edit,new`), `LOG_LINES` (`DASHBOARD_LOG_LINES`, default `60`), `BOT_HISTORY_WINDOWS` (default `12`). Add `KAFKA_TOPIC`, `DASHBOARD_LOG_WIKIS` and `DASHBOARD_LOG_LINES` to the `dashboard` service in `docker-compose.yml` and to `.env.example`.

- [ ] **Step 4: Run the tests to verify they pass, then check the endpoint against the live stack**

Run: `python tests/dashboard_selfcheck.py`
Expected: `DASHBOARD SELFCHECK OK`

Run: `podman compose --profile dashboard up -d --force-recreate dashboard` then `curl -s http://localhost:8080/api/state`
Expected: JSON whose `log` array fills with `enwiki` and `viwiki` entries within about a minute, and whose `kpi` is populated.

- [ ] **Step 5: Commit**

```bash
git add src/dashboard/dashboard.py tests/dashboard_selfcheck.py docker-compose.yml .env.example
git commit -m "Serve the dashboard view model as JSON and consume the raw topic"
```

---

### Task 4: The page

**Files:**
- Modify: `src/dashboard/dashboard.py`
- Test: `tests/dashboard_selfcheck.py`

**Interfaces:**
- Consumes: `state_json` from Task 3.
- Produces: `render_page(state: dict, now: str) -> str` — the shell, the inline CSS, the inline script, and the `<noscript>` server-rendered fallback.

Read the `dataviz` skill before writing the chart code and `ui-ux-pro-max:ui-styling` before fixing the palette and type scale.

- [ ] **Step 1: Write the failing tests**

```python
page = render_page(state, "12:00:00")
assert "<noscript>" in page and "Chưa có window nào đóng" in render_page(new_state(), "12:00:00")
assert "/api/state" in page
assert "<script>alert(1)</script>" not in page and "&lt;script&gt;" in page
assert "http://" not in page.split("<noscript>")[0]   # không tải tài nguyên ngoài
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `python tests/dashboard_selfcheck.py`
Expected: FAIL on the `/api/state` assertion

- [ ] **Step 3: Rewrite `render_page`**

Seven blocks in the order the spec fixes: KPI row, stacked edits-per-minute chart, top wikis with a bot/human split bar, bot ratio trend line, spike feed, live log, pipeline status bar. The client script fetches `/api/state` every 2 seconds and patches the DOM; it sets text through `textContent`, never `innerHTML`, so a page title cannot inject markup. Charts are inline SVG built from the JSON. Colors are CSS custom properties on `:root` with a `prefers-color-scheme: dark` block. The `<noscript>` body keeps the current server-rendered blocks unchanged.

- [ ] **Step 4: Run the tests to verify they pass, then look at the real page**

Run: `python tests/dashboard_selfcheck.py`
Expected: `DASHBOARD SELFCHECK OK`

Recreate the container and open `http://localhost:8080`. Expected: the log fills with readable `enwiki` and `viwiki` titles, charts hold real values, and nothing flickers or jumps to the top of the page.

- [ ] **Step 5: Commit**

```bash
git add src/dashboard/dashboard.py tests/dashboard_selfcheck.py
git commit -m "Rebuild the dashboard page around the JSON endpoint"
```

---

### Task 5: Documentation

**Files:**
- Modify: `README.md`, `docs/02_architecture.md`, `docs/03_data_contract.md`, `docs/08_demo_script.md`

- [ ] **Step 1: Update the documents**

`02_architecture.md`: the dashboard now reads both topics — add it to the diagram and add a row to the decision table saying the raw read exists only for the log and that it starts at `latest`. `03_data_contract.md`: the line forbidding account names on the dashboard stays, with a note that the log shows masked names. `08_demo_script.md`: step 5 gains the log, and a sentence saying the log only holds events from when the page opened. `README.md`: mention the log in the dashboard section.

- [ ] **Step 2: Commit**

```bash
git add README.md docs/
git commit -m "Document the dashboard log and the raw topic read"
```
