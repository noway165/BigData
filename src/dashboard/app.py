"""Streamlit Dashboard: đọc topic `wikipedia-analytics` và hiển thị theo thời gian thực.

Dashboard chỉ đọc kết quả Spark đã tổng hợp, không đọc raw stream và không tính lại,
nên không làm chậm Spark: hai bên chỉ gặp nhau qua Kafka.

Chạy trên máy host (Kafka mở cổng 9094):
    pip install -r src/dashboard/requirements.txt
    streamlit run src/dashboard/app.py

Chạy trong Docker (profile dashboard, đọc kafka:9092):
    docker compose --profile dashboard up -d   # mở http://localhost:8501

Chạy không cần Kafka, từ file JSONL đã lưu (dự phòng khi demo):
    ANALYTICS_SOURCE_FILE=tests/sample_events/analytics_sample.jsonl streamlit run src/dashboard/app.py
"""
from __future__ import annotations

import json
import os
import sys
import threading
import time
import uuid

import altair as alt
import pandas as pd
import streamlit as st

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import analytics_view as av  # noqa: E402

env = os.environ.get
BOOTSTRAP = env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
TOPIC = env("KAFKA_ANALYTICS_TOPIC", "wikipedia-analytics")
RAW_TOPIC = env("KAFKA_TOPIC", "wikipedia-recent-changes")
SOURCE_FILE = env("ANALYTICS_SOURCE_FILE", "")
REFRESH_S = float(env("DASHBOARD_REFRESH_S", "5"))
# Giới hạn bộ nhớ: topic giữ 24 giờ, Dashboard chỉ cần phần gần nhất.
MAX_RECORDS = int(env("DASHBOARD_MAX_RECORDS", "200000"))

CATEGORY_COLORS = alt.Scale(domain=["Bot", "Human", "Unknown"],
                            range=["#E07B39", "#3B7DD8", "#9AA0A6"])


class AnalyticsFeed:
    """Một consumer dùng chung cho mọi phiên trình duyệt; giữ record đã đọc trong RAM.

    group.id ngẫu nhiên và không commit offset: mỗi lần khởi động Dashboard đọc lại
    topic từ đầu, nên mở lại trang vẫn thấy lịch sử, và Dashboard không ảnh hưởng
    tiến độ của consumer nào khác.
    """

    def __init__(self):
        self.lock = threading.Lock()
        self.records: list[dict] = []
        self.stats = av.ParseStats()
        self.error = ""
        self.last_message_at: float | None = None
        self.kafka_total: int | None = None
        self.total_at_last_result: int | None = None
        self.consumer = None
        if SOURCE_FILE:
            self._load_file(SOURCE_FILE)
        else:
            self._connect()

    def _connect(self):
        try:
            from confluent_kafka import Consumer
            self.consumer = Consumer({
                "bootstrap.servers": BOOTSTRAP,
                "group.id": f"dashboard-{uuid.uuid4()}",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            })
            self.consumer.subscribe([TOPIC])
        except Exception as exc:  # thiếu thư viện hoặc cấu hình sai: hiện lỗi trên trang
            self.error = f"Không tạo được Kafka consumer: {exc}"

    def _load_file(self, path):
        with open(path, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                item = json.loads(line)
                # Chấp nhận cả {"key":..., "value": {...}} lẫn value trần.
                key, value = (item.get("key"), item.get("value")) if "value" in item else (None, item)
                if isinstance(value, dict):
                    value = json.dumps(value)
                rec = av.parse_message(key, value, self.stats)
                if rec:
                    self.records.append(rec)
        self.last_message_at = time.time()

    def poll(self, budget_s: float = 1.0, batch: int = 500):
        if self.consumer is None:
            return
        deadline = time.time() + budget_s
        with self.lock:
            before = self.last_message_at
            while time.time() < deadline:
                msgs = self.consumer.consume(num_messages=batch, timeout=0.2)
                if not msgs:
                    break
                for m in msgs:
                    if m.error():
                        # Partition EOF không phải lỗi; lỗi khác thì ghi lại để hiển thị.
                        if m.error().code() != -191:
                            self.error = str(m.error())
                        continue
                    rec = av.parse_message(m.key(), m.value(), self.stats)
                    if rec:
                        self.records.append(rec)
                        self.last_message_at = time.time()
            if len(self.records) > MAX_RECORDS:
                del self.records[: len(self.records) - MAX_RECORDS]
            self.kafka_total = self._raw_topic_total()
            if self.last_message_at != before:
                self.total_at_last_result = self.kafka_total

    def _raw_topic_total(self) -> int | None:
        """Tổng event Kafka đang giữ ở topic raw, hoặc None nếu không hỏi được broker."""
        from confluent_kafka import TopicPartition
        try:
            partitions = self.consumer.list_topics(RAW_TOPIC, timeout=2).topics[RAW_TOPIC].partitions
            return sum(self.consumer.get_watermark_offsets(TopicPartition(RAW_TOPIC, p), timeout=2)[1]
                       for p in partitions)
        except Exception:  # Kafka tắt là tình huống phải hiện trên trang, không được làm sập trang
            return None

    def snapshot(self) -> list[dict]:
        with self.lock:
            return list(self.records)


@st.cache_resource
def get_feed() -> AnalyticsFeed:
    return AnalyticsFeed()


def utc_naive(df: pd.DataFrame) -> pd.DataFrame:
    """Bỏ múi giờ trước khi vẽ: Vega-Lite đổi timestamp có múi giờ sang giờ của trình duyệt
    (ở Việt Nam là UTC+7), trong khi trục ghi UTC. Timestamp không múi giờ được vẽ nguyên giá trị."""
    df = df.copy()
    for col in ("window_start", "window_end"):
        if col in df.columns and isinstance(df[col].dtype, pd.DatetimeTZDtype):
            df[col] = df[col].dt.tz_convert("UTC").dt.tz_localize(None)
    return df


def fmt_time(ts) -> str:
    return "—" if ts is None or pd.isna(ts) else pd.Timestamp(ts).strftime("%H:%M:%S UTC")


st.set_page_config(page_title="Wikipedia Recent Changes – Real-time", layout="wide")
# Streamlit làm mờ nội dung mỗi lần làm mới; trên màn chiếu nền sáng trông như trang bị treo.
st.markdown("<style>[data-stale='true'] {opacity: 1 !important;}</style>", unsafe_allow_html=True)
st.title("Wikipedia Recent Changes – Real-time Analytics")
st.caption(
    f"Nguồn: Kafka topic `{TOPIC}` do Spark Structured Streaming ghi. "
    "Mọi window tính theo **event time** (`meta.dt`), giờ UTC. "
    "Dashboard không hiển thị tên tài khoản người dùng."
)

with st.sidebar:
    st.header("Cấu hình")
    source = f"File `{SOURCE_FILE}`" if SOURCE_FILE else f"Kafka `{BOOTSTRAP}` / `{TOPIC}`"
    st.write(f"**Nguồn:** {source}")
    st.write(f"**Làm mới:** mỗi {REFRESH_S:g} giây")
    top_n_windows = st.slider("Top wiki: số window 1 phút gần nhất", 1, 60, 10)
    top_k = st.slider("Top wiki: số wiki hiển thị", 5, 20, 10)
    spike_minutes = st.slider("Spike: xem trong bao nhiêu phút gần nhất", 5, 120, 30)
    hide_bot_spikes = st.checkbox("Ẩn spike chủ yếu do bot", value=False)


def render_pipeline_status(feed: AnalyticsFeed):
    """Dải trạng thái để thấy ngay Spark chết và Kafka vẫn giữ dữ liệu khi demo recovery."""
    age = None if feed.last_message_at is None else time.time() - feed.last_message_at
    state, held = av.pipeline_status(age, feed.kafka_total, feed.total_at_last_result)
    if state == "kafka_down":
        st.error("Không truy cập được Kafka.", icon=":material/error:")
    elif state == "stale":
        st.error(f"Spark đã ngừng phát kết quả {age:.0f} giây. Kafka vẫn đang giữ dữ liệu.",
                 icon=":material/warning:")
    elif state == "ok":
        st.success("Spark đang xử lý bình thường.", icon=":material/check_circle:")
    c1, c2, c3 = st.columns(3)
    c1.metric("Kafka đã nhận", "—" if feed.kafka_total is None else f"{feed.kafka_total:,}")
    c2.metric("Kết quả Spark mới nhất", "—" if age is None else f"{age:.0f} giây trước")
    c3.metric("Kafka giữ chờ Spark", f"+{held:,}" if held else "0")


@st.fragment(run_every=REFRESH_S)
def render():
    feed = get_feed()
    feed.poll()
    records = feed.snapshot()
    frames = av.to_frames(records)
    edit_df, bot_df, spike_df = frames["edit_count"], frames["bot_ratio"], frames["activity_spike"]

    if feed.error:
        st.error(feed.error)
    if not records:
        st.info(
            "Chưa có kết quả analytics. Spark dùng `outputMode(append)` nên một window chỉ được "
            "phát khi đã đóng: chờ qua độ dài window + watermark (khoảng 2–3 phút với window "
            "1 phút, watermark 1 phút). Kiểm tra bằng `sh scripts/consume_analytics.sh 10`."
        )
        return

    if not SOURCE_FILE:
        render_pipeline_status(feed)

    # ---- Hàng chỉ số tổng quan ----
    window, total, delta = av.latest_total(edit_df)
    breakdown = av.bot_breakdown(bot_df)
    spikes = av.spike_table(spike_df, spike_minutes)
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Event trong window 1 phút mới nhất", f"{total:,}",
              None if delta is None else f"{delta:+,} so với phút trước")
    c1.caption(f"Window bắt đầu {fmt_time(window)}")
    human = breakdown.set_index("category")["share"].get("Human") if not breakdown.empty else None
    c2.metric("Tỷ lệ Human (window 5 phút)", "—" if human is None else f"{human:.1%}")
    c3.metric(f"Bài viết có spike ({spike_minutes} phút)", f"{len(spikes):,}")
    c4.metric("Số wiki đang hoạt động", f"{edit_df[edit_df['window_start'] == window]['wiki'].nunique() if window is not None else 0:,}")

    counted = edit_df["counted_types"].iloc[-1] if "counted_types" in edit_df.columns and not edit_df.empty else "all"
    if counted == "all":
        st.caption("Đang đếm **mọi** loại event (`edit`, `new`, `categorize`, `log`). "
                   "Muốn chỉ đếm lượt sửa thật, chạy analytics với `ANALYTICS_EVENT_TYPES=edit,new`.")
    else:
        st.caption(f"Chỉ đếm event loại: `{counted}`.")

    # ---- Biểu đồ theo thời gian và top wiki ----
    left, right = st.columns([3, 2])
    with left:
        st.subheader("Số event mỗi phút (event time)")
        timeline = av.timeline_with_gaps(av.edits_timeline(edit_df))
        long = timeline.melt(id_vars="window_start", value_vars=["bot_edits", "non_bot_edits"],
                             var_name="loại", value_name="event")
        long["loại"] = long["loại"].map({"bot_edits": "Bot", "non_bot_edits": "Human + Unknown"})
        chart = (alt.Chart(utc_naive(long)).mark_area(opacity=0.85)
                 .encode(x=alt.X("window_start:T", title="Window bắt đầu (UTC)", axis=alt.Axis(format="%H:%M")),
                         y=alt.Y("event:Q", stack=True, title="Số event"),
                         color=alt.Color("loại:N", scale=alt.Scale(
                             domain=["Bot", "Human + Unknown"], range=["#E07B39", "#3B7DD8"])),
                         tooltip=["window_start:T", "loại:N", "event:Q"]))
        st.altair_chart(chart, use_container_width=True)
    with right:
        st.subheader(f"Top {top_k} wiki ({top_n_windows} phút gần nhất)")
        top = av.top_wikis(edit_df, top_n_windows, top_k)
        bars = (alt.Chart(top).mark_bar(color="#3B7DD8")
                .encode(x=alt.X("edit_count:Q", title="Số event"),
                        y=alt.Y("wiki:N", sort="-x", title=None),
                        tooltip=["wiki", "edit_count", "bot_edits",
                                 alt.Tooltip("bot_share:Q", format=".1%")]))
        st.altair_chart(bars, use_container_width=True)

    # ---- Bot / Human / Unknown ----
    left, right = st.columns([2, 3])
    with left:
        st.subheader("Bot / Human / Unknown")
        if breakdown.empty:
            st.info("Chưa có window 5 phút nào đóng.")
        else:
            last_bot_window = bot_df["window_start"].max()
            pie = (alt.Chart(breakdown).mark_arc(innerRadius=60)
                   .encode(theta="count:Q",
                           color=alt.Color("category:N", scale=CATEGORY_COLORS, title=None),
                           tooltip=["category", "count", alt.Tooltip("share:Q", format=".1%")]))
            st.altair_chart(pie, use_container_width=True)
            st.caption(f"Window 5 phút bắt đầu {fmt_time(last_bot_window)}. "
                       "Mẫu số: tổng event của window, kể cả Unknown.")
    with right:
        st.subheader("Tỷ lệ theo thời gian")
        hist = av.bot_ratio_history(bot_df)
        if not hist.empty:
            lines = (alt.Chart(utc_naive(hist)).mark_line(point=True)
                     .encode(x=alt.X("window_start:T", title="Window 5 phút (UTC)", axis=alt.Axis(format="%H:%M")),
                             y=alt.Y("share:Q", axis=alt.Axis(format="%"), title="Tỷ lệ"),
                             color=alt.Color("category:N", scale=CATEGORY_COLORS, title=None)))
            st.altair_chart(lines, use_container_width=True)

    # ---- Activity spike ----
    st.subheader("Cảnh báo: hoạt động chỉnh sửa tăng đột biến")
    threshold = int(spike_df["threshold_edits"].iloc[-1]) if "threshold_edits" in spike_df.columns and not spike_df.empty else 10
    st.caption(
        f"Bài viết có hơn **{threshold}** lượt sửa trong cửa sổ trượt 2 phút (bước 30 giây). "
        "Đây là ngưỡng heuristic: nhiều lượt sửa **không** chứng minh có tranh chấp (edit war). "
        "Cột *Do bot* đánh dấu đợt mà hơn một nửa số lượt sửa là của bot."
    )
    shown = spikes[~spikes["likely_bot_driven"]] if hide_bot_spikes else spikes
    if shown.empty:
        st.info("Không có bài viết nào vượt ngưỡng trong khoảng thời gian đang xem.")
    else:
        table = shown.copy()
        table.insert(2, "Lịch sử", [av.history_url(w, t) for w, t in zip(table["wiki"], table["title"])])
        st.dataframe(
            table.rename(columns={
                "wiki": "Wiki", "title": "Bài viết", "edit_count": "Lượt sửa (2 phút)",
                "distinct_users": "Người sửa (≈)", "bot_edits": "Bot", "human_edits": "Human",
                "window_start": "Từ", "window_end": "Đến", "windows_flagged": "Số window vượt ngưỡng",
                "likely_bot_driven": "Do bot"}),
            hide_index=True, use_container_width=True,
            column_config={"Lịch sử": st.column_config.LinkColumn(display_text="Mở")})

    # ---- Độ trễ và tình trạng pipeline ----
    with st.expander("Độ trễ phát kết quả và tình trạng dữ liệu"):
        delay = av.result_delay_seconds(frames)
        st.write("**Độ trễ phát kết quả** = `emitted_at − window_end`: window đóng xong bao lâu "
                 "thì kết quả tới Kafka (gồm chờ watermark và trigger). Latency từng event "
                 "đo riêng trên raw Parquet, xem `latency_report.md`.")
        st.dataframe(delay, hide_index=True)
        s = feed.stats
        st.write(
            f"Message đã đọc: **{s.received:,}** · JSON hỏng: **{s.bad_json}** · "
            f"metric lạ: **{s.unknown_metric}** · bản ghi lặp đã khử: "
            f"**{av.duplicates_removed(records, frames)}** "
            f"(Kafka sink là at-least-once nên có thể có bản lặp sau khi Spark chạy lại)."
        )
        st.write(f"Message mới nhất nhận lúc: "
                 f"{time.strftime('%H:%M:%S', time.localtime(feed.last_message_at)) if feed.last_message_at else '—'}")


render()
