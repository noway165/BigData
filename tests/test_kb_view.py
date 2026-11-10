"""Test logic tra cứu knowledge base (không cần Spark). Chạy: python -m pytest tests -q"""
import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src" / "kb"))

import app as kb  # noqa: E402

ARTICLES = pd.DataFrame([
    {"wiki": "viwiki", "title": "Việt Nam", "edits": 5},
    {"wiki": "enwiki", "title": "Vietnam (country)", "edits": 9},
    {"wiki": "enwiki", "title": "Python", "edits": 1},
])


def test_search_ignores_case_and_sorts_by_edits():
    assert list(kb.search_articles(ARTICLES, "vIeT")["title"]) == ["Vietnam (country)"]
    assert list(kb.search_articles(ARTICLES)["edits"]) == [9, 5, 1]


def test_search_treats_regex_characters_literally():
    assert list(kb.search_articles(ARTICLES, "(country")["title"]) == ["Vietnam (country)"]


def test_search_filters_by_wiki_and_blank_query_keeps_all():
    assert list(kb.search_articles(ARTICLES, "   ", "viwiki")["title"]) == ["Việt Nam"]
    assert kb.search_articles(ARTICLES, "không có").empty


def test_spikes_of_only_returns_that_article_in_time_order():
    spikes = pd.DataFrame([
        {"wiki": "enwiki", "title": "A", "started_at": "2026-11-10 10:05:00"},
        {"wiki": "enwiki", "title": "A", "started_at": "2026-11-10 10:00:00"},
        {"wiki": "viwiki", "title": "A", "started_at": "2026-11-10 09:00:00"},
    ])
    found = kb.spikes_of(spikes, "enwiki", "A")
    assert list(found["started_at"]) == ["2026-11-10 10:00:00", "2026-11-10 10:05:00"]
    assert kb.spikes_of(pd.DataFrame(), "enwiki", "A").empty


def test_with_links_builds_history_url():
    assert "en.wikipedia.org" in kb.with_links(ARTICLES.tail(1)).iloc[0]["link"]


def test_load_table_keeps_numeric_looking_titles_as_text(tmp_path):
    path = tmp_path / "article_profile.jsonl"
    path.write_text(json.dumps({"wiki": "enwiki", "title": "1984", "edits": 2}) + "\n", "utf-8")
    assert kb.load_table(path).iloc[0]["title"] == "1984"


def test_load_table_handles_missing_and_empty_files(tmp_path):
    assert kb.load_table(tmp_path / "missing.jsonl").empty
    empty = tmp_path / "empty.jsonl"
    empty.write_text("", "utf-8")
    assert kb.load_table(empty).empty
