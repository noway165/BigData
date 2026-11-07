"""Producer unit tests. Run from the repo root: python -m pytest tests -q"""
import json
import sys
import urllib.error
from datetime import datetime, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src" / "producer"))

from config import Config  # noqa: E402
import sse_kafka_producer as sut  # noqa: E402

SAMPLES = (ROOT / "tests" / "sample_events" / "recentchange_sample.jsonl").read_text("utf-8").splitlines()
NOW = datetime(2026, 11, 7, 16, 20, 18, 500000, tzinfo=timezone.utc)


class FakeProducer:
    def __init__(self, full_times=0):
        self.sent = []
        self.full_times = full_times
        self.polls = 0

    def produce(self, topic, key, value, callback):
        if self.full_times:
            self.full_times -= 1
            raise BufferError
        self.sent.append((topic, key, json.loads(value)))
        callback(None, None)

    def poll(self, _timeout):
        self.polls += 1

    def __len__(self):
        return 0


class Stop(Exception):
    pass


def sse(*blocks):
    return [line for block in blocks for line in (block + "\n\n").encode().splitlines(True)]


def stream(lines):
    class Stream(list):
        def __enter__(self):
            return self

        def __exit__(self, *_):
            return False
    return Stream(lines)


def test_iter_sse_reads_id_type_and_skips_comments():
    lines = sse(":ok", 'event: message\nid: [1]\ndata: {"a":1}', "event: error\ndata: boom")
    assert list(sut.iter_sse(lines)) == [("message", "[1]", '{"a":1}'), ("error", None, "boom")]


def test_iter_sse_joins_multiline_data_and_handles_crlf():
    assert list(sut.iter_sse([b"data: a\r\n", b"data: b\r\n", b"\r\n"])) == [("message", None, "a\nb")]


@pytest.mark.parametrize("data", SAMPLES)
def test_real_samples_map_to_contract(data):
    event = json.loads(data)
    key, envelope = sut.build_message(sut.parse_event(data), data, NOW)
    assert key == f"{event['wiki']}:{event['title']}"
    assert envelope == {
        "event_id": event["meta"]["id"],
        "event_time": event["meta"]["dt"],
        "producer_received_at": "2026-11-07T16:20:18.500Z",
        "raw_value": data,
    }


def test_missing_fields_become_null_not_a_crash():
    key, envelope = sut.build_message({"type": "edit", "meta": "bad"}, "{}", NOW)
    assert key is None and envelope["event_id"] is None and envelope["event_time"] is None


@pytest.mark.parametrize("data", ["not json", "[1,2]", "3"])
def test_bad_payload_is_rejected(data):
    with pytest.raises(ValueError):
        sut.parse_event(data)


def test_handle_event_counts_every_outcome():
    producer, stats = FakeProducer(), sut.Stats(30)
    cfg = Config(kafka_topic="t")
    sut.handle_event(cfg, producer, stats, "message", SAMPLES[0])
    sut.handle_event(cfg, producer, stats, "message", '{"meta":{"domain":"canary"}}')
    sut.handle_event(cfg, producer, stats, "message", "broken")
    sut.handle_event(cfg, producer, stats, "error", "upstream failed")
    sut.handle_event(cfg, producer, stats, "message", '{"type":"edit"}')
    c = stats.counts
    assert (c["received"], c["sent"], c["canary_skipped"], c["malformed"]) == (4, 2, 1, 1)
    assert (c["sse_error_events"], c["delivered"], c["missing_event_id"], c["missing_event_time"]) == (1, 2, 1, 1)
    assert len(producer.sent) == 2 and len(stats.latencies) == 1


def test_full_queue_waits_and_keeps_the_event():
    producer, stats = FakeProducer(full_times=2), sut.Stats(30)
    sut.publish(producer, "t", "k", {"a": 1}, stats)
    assert producer.sent == [("t", "k", {"a": 1})]
    assert stats.counts["queue_full_waits"] == 2


def test_reconnect_resumes_from_last_id_after_close_error_and_429():
    data = SAMPLES[0]
    calls, waits = [], []
    rate_limited = urllib.error.HTTPError("u", 429, "slow", {"Retry-After": "7"}, None)
    script = [
        lambda: stream(sse(f"id: A\ndata: {data}")),  # server closes after one event
        lambda: (_ for _ in ()).throw(TimeoutError("read timed out")),
        lambda: (_ for _ in ()).throw(rate_limited),
        lambda: stream(sse(f"id: B\ndata: {data}")),
        lambda: (_ for _ in ()).throw(Stop()),
    ]

    def fake_open(_cfg, last_id):
        calls.append(last_id)
        return script[len(calls) - 1]()

    producer, stats = FakeProducer(), sut.Stats(30)
    with pytest.raises(Stop):
        sut.run(Config(kafka_topic="t"), producer, stats, open_stream=fake_open, sleep=waits.append)

    assert calls == [None, "A", "A", "A", "B"]
    assert len(producer.sent) == 2 and stats.counts["reconnects"] == 4
    assert waits[2] == 7  # honours Retry-After


def test_backoff_doubles_to_the_cap():
    waits = []
    errors = [OSError("down")] * 5

    def fake_open(_cfg, _last_id):
        if errors:
            raise errors.pop()
        raise Stop()

    with pytest.raises(Stop):
        sut.run(Config(backoff_min_s=1, backoff_max_s=4), FakeProducer(), sut.Stats(30),
                open_stream=fake_open, sleep=waits.append)
    # jitter is 50-100% of the current step
    for wait, step in zip(waits, [1, 2, 4, 4, 4]):
        assert step * 0.5 <= wait <= step
