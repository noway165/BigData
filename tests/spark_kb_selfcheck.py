"""Spark checks for the knowledge base job. Run in the spark container:
    /opt/spark/bin/spark-submit --master "local[1]" /opt/app/tests/spark_kb_selfcheck.py
"""
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src" / "streaming"))

from pyspark.sql import SparkSession  # noqa: E402

import build_knowledge_base as kb  # noqa: E402
from raw_to_hdfs import to_raw_rows  # noqa: E402

spark = (SparkSession.builder.master("local[1]").appName("kb_selfcheck")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", "2").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")


def event(event_id, minute, second, wiki="enwiki", title="A", kind="edit", user="u1", bot=False):
    change = {"wiki": wiki, "title": title, "type": kind, "namespace": 0, "user": user, "bot": bot, "timestamp": 1}
    return json.dumps({"event_id": event_id, "event_time": f"2026-11-10T10:{minute:02d}:{second:02d}.000Z",
                       "producer_received_at": "2026-11-10T11:00:00.000Z", "raw_value": json.dumps(change)})


messages = [event(f"a{i}", 0, i * 2, user=f"u{i % 2}") for i in range(12)]  # burst 1 on A
messages += [event(f"late{i}", 30, i * 2) for i in range(12)]  # burst 2 on A, far from burst 1
messages += [messages[0]]  # redelivered, must not count
messages += [event("b1", 0, 1, title="B", bot=True), event("c1", 0, 1, wiki="viwiki", title="A")]
messages += [event("x1", 0, 1, title="Category:X", kind="categorize"), "not json"]  # excluded

raw = to_raw_rows(spark.createDataFrame([(m.encode(), 0, i) for i, m in enumerate(messages)],
                                        ["value", "partition", "offset"]))
events = kb.clean_events(raw).cache()
assert events.count() == 26, events.count()

spikes = kb.spike_history(events).cache()
rows = spikes.orderBy("started_at").collect()
assert len(rows) == 2, rows  # overlapping sliding windows merge into one burst each
assert (rows[0].wiki, rows[0].title, rows[0].peak_edits) == ("enwiki", "A", 12)

articles = {(r.wiki, r.title): r for r in kb.article_profile(events, spikes).collect()}
assert set(articles) == {("enwiki", "A"), ("enwiki", "B"), ("viwiki", "A")}
a = articles[("enwiki", "A")]
assert (a.edits, a.editors, a.spike_bursts, a.human_edits, a.bot_edits) == (24, 2, 2, 24, 0)
assert articles[("enwiki", "B")].spike_bursts == 0 and articles[("enwiki", "B")].bot_edits == 1

wikis = {r.wiki: r for r in kb.wiki_profile(events).collect()}
assert (wikis["enwiki"].edits, wikis["enwiki"].articles) == (25, 2)
assert wikis["enwiki"].bot_share == round(1 / 25, 4)
assert sum(r.edits for r in wikis.values()) == events.count()

spark.stop()
print("KB SELFCHECK OK")
