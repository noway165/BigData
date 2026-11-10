"""Trang tra cứu knowledge base về hoạt động chỉnh sửa Wikipedia.

Chạy từ thư mục gốc repo, sau khi đã chạy scripts/build_kb.sh:
    streamlit run src/kb/app.py --server.port 8502
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src" / "dashboard"))

from analytics_view import history_url  # noqa: E402

KB_DIR = Path(os.environ.get("KB_DIR", ROOT / "outputs" / "kb"))
MAX_ROWS = 200
ARTICLE_LABELS = {"wiki": "Wiki", "title": "Bài viết", "link": "Lịch sử", "edits": "Lượt sửa",
                  "bot_edits": "Bot", "human_edits": "Human", "editors": "Người sửa",
                  "spike_bursts": "Đợt đột biến", "first_seen": "Lần đầu (UTC)", "last_seen": "Lần cuối (UTC)"}
WIKI_LABELS = {"wiki": "Wiki", "edits": "Lượt sửa", "articles": "Số bài", "editors": "Người sửa",
               "bot_share": "Tỷ lệ bot", "first_seen": "Lần đầu (UTC)", "last_seen": "Lần cuối (UTC)"}
SPIKE_LABELS = {"wiki": "Wiki", "title": "Bài viết", "link": "Lịch sử", "peak_edits": "Lượt sửa cao nhất (2 phút)",
                "peak_editors": "Người sửa (≈)", "peak_bot_edits": "Bot", "peak_human_edits": "Human",
                "started_at": "Bắt đầu (UTC)", "ended_at": "Kết thúc (UTC)"}


def load_table(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    # Parse từng dòng: read_json đoán kiểu, nên sẽ đổi tiêu đề bài "1984" thành số.
    return pd.DataFrame(json.loads(line) for line in path.read_text("utf-8").splitlines())


def search_articles(articles: pd.DataFrame, query: str = "", wiki: str | None = None) -> pd.DataFrame:
    """Tìm theo một phần tên bài, không phân biệt hoa thường; ký tự như ( ) được hiểu nguyên văn."""
    found = articles
    if wiki:
        found = found[found["wiki"] == wiki]
    if query.strip():
        found = found[found["title"].str.contains(query.strip(), case=False, regex=False, na=False)]
    return found.sort_values(["edits", "title"], ascending=[False, True])


def spikes_of(spikes: pd.DataFrame, wiki: str, title: str) -> pd.DataFrame:
    if spikes.empty:
        return spikes
    return spikes[(spikes["wiki"] == wiki) & (spikes["title"] == title)].sort_values("started_at")


def with_links(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(link=[history_url(w, t) for w, t in zip(df["wiki"], df["title"])])


def show_table(df: pd.DataFrame, labels: dict):
    st.dataframe(df[list(labels)].rename(columns=labels), hide_index=True, use_container_width=True,
                 column_config={"Lịch sử": st.column_config.LinkColumn(display_text="Mở")})


@st.cache_data(ttl=30)
def load_kb():
    meta_path = KB_DIR / "meta.json"
    meta = json.loads(meta_path.read_text("utf-8")) if meta_path.exists() else None
    return meta, *(load_table(KB_DIR / f"{name}.jsonl")
                   for name in ("wiki_profile", "article_profile", "spike_history"))


def main():
    st.set_page_config(page_title="Wikipedia Knowledge Base", layout="wide")
    st.title("Knowledge Base: hoạt động chỉnh sửa Wikipedia")
    meta, wikis, articles, spikes = load_kb()
    if meta is None or articles.empty:
        st.info("Chưa có dữ liệu. Chạy `podman exec spark sh /opt/app/scripts/build_kb.sh` rồi tải lại trang.")
        return

    start, end = meta["event_time_range"]
    st.caption(f"Dựng từ raw Parquet trên HDFS lúc {meta['built_at']}. Dữ liệu từ {start} đến {end} UTC. "
               "Chỉ gồm lượt sửa thật (`edit`, `new`), không chứa tên người dùng hay nội dung bài viết.")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Lượt sửa", f"{meta['edits']:,}")
    c2.metric("Bài viết", f"{meta['rows']['article_profile']:,}")
    c3.metric("Wiki", f"{meta['rows']['wiki_profile']:,}")
    c4.metric("Đợt đột biến", f"{meta['rows']['spike_history']:,}")

    tab_articles, tab_wikis, tab_spikes = st.tabs(["Tra cứu bài viết", "Wiki", "Đợt đột biến"])
    with tab_articles:
        left, right = st.columns([3, 1])
        query = left.text_input("Tên bài viết", placeholder="Ví dụ: Việt Nam")
        wiki = right.selectbox("Wiki", ["Tất cả", *sorted(articles["wiki"].unique())])
        found = search_articles(articles, query, None if wiki == "Tất cả" else wiki)
        st.write(f"Tìm thấy **{len(found):,}** bài. Hiển thị tối đa {MAX_ROWS}.")
        if found.empty:
            return
        top = with_links(found.head(MAX_ROWS))
        show_table(top, ARTICLE_LABELS)

        st.subheader("Hồ sơ bài viết")
        options = list(zip(top["wiki"], top["title"]))
        picked_wiki, picked_title = st.selectbox("Chọn bài", options, format_func=lambda o: f"{o[1]} ({o[0]})")
        row = top[(top["wiki"] == picked_wiki) & (top["title"] == picked_title)].iloc[0]
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Lượt sửa", f"{int(row['edits']):,}")
        c2.metric("Bot / Human", f"{int(row['bot_edits']):,} / {int(row['human_edits']):,}")
        c3.metric("Người sửa", f"{int(row['editors']):,}")
        c4.metric("Đợt đột biến", f"{int(row['spike_bursts']):,}")
        st.write(f"Lần đầu thấy: {row['first_seen']} UTC. Lần cuối: {row['last_seen']} UTC.")
        if row["link"]:
            st.markdown(f"[Mở lịch sử sửa trên Wikimedia]({row['link']})")
        history = spikes_of(spikes, picked_wiki, picked_title)
        if not history.empty:
            show_table(with_links(history), SPIKE_LABELS)

    with tab_wikis:
        show_table(wikis, WIKI_LABELS)

    with tab_spikes:
        st.caption("Nhiều lượt sửa không chứng minh có tranh chấp (edit war). Đây là đột biến hoạt động.")
        if spikes.empty:
            st.info("Chưa có đợt đột biến nào.")
        else:
            show_table(with_links(spikes.head(MAX_ROWS)), SPIKE_LABELS)


if __name__ == "__main__":
    main()
