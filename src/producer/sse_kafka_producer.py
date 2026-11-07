"""Read Wikimedia EventStreams (SSE) and publish each event to Kafka.

Message contract: docs/03_data_contract.md. Failure behaviour: docs/07_ingestion.md.
"""
import http.client
import json
import logging
import random
import signal
import statistics
import sys
import time
import urllib.error
import urllib.request
from collections import Counter
from datetime import datetime, timezone

from confluent_kafka import Producer

from config import Config

log = logging.getLogger("producer")


def iter_sse(lines):
    """Yield (event_type, event_id, data) from raw SSE lines."""
    event_type, event_id, data = "message", None, []
    for raw in lines:
        line = raw.decode("utf-8", "replace").rstrip("\r\n")
        if not line:
            if data:
                yield event_type, event_id, "\n".join(data)
            event_type, event_id, data = "message", None, []
        elif not line.startswith(":"):
            field, _, value = line.partition(":")
            value = value[1:] if value.startswith(" ") else value
            if field == "data":
                data.append(value)
            elif field == "id":
                event_id = value
            elif field == "event":
                event_type = value


def to_utc_iso(moment: datetime) -> str:
    """Same shape as meta.dt, so Spark parses both timestamps identically."""
    return moment.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def parse_time(value):
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (AttributeError, ValueError):
        return None


def parse_event(data: str) -> dict:
    """Raise ValueError if data is not a JSON object."""
    event = json.loads(data)
    if not isinstance(event, dict):
        raise ValueError("event is not a JSON object")
    return event


def event_meta(event: dict) -> dict:
    return event["meta"] if isinstance(event.get("meta"), dict) else {}


def build_message(event: dict, data: str, received_at: datetime):
    """Return (key, envelope). Missing fields become null; Spark and the report count them."""
    meta = event_meta(event)
    # wiki:title keeps per-article order and spreads load better than wiki alone,
    # where commonswiki and wikidatawiki would dominate one partition.
    if event.get("wiki") and event.get("title"):
        key = f"{event['wiki']}:{event['title']}"
    else:
        key = meta.get("id")
    return key, {
        "event_id": meta.get("id"),
        "event_time": meta.get("dt"),
        "producer_received_at": to_utc_iso(received_at),
        "raw_value": data,
    }


class Stats:
    """Counters plus latency samples; logged and reset for latency every interval."""

    def __init__(self, interval_s: float):
        self.interval_s = interval_s
        self.counts = Counter()
        self.latencies = []
        self.last_report = time.monotonic()

    def observe(self, envelope: dict, received_at: datetime):
        self.counts["sent"] += 1
        self.counts["missing_event_id"] += envelope["event_id"] is None
        event_time = parse_time(envelope["event_time"])
        if event_time is None:
            self.counts["missing_event_time"] += 1
        else:
            self.latencies.append((received_at - event_time).total_seconds())

    def report_if_due(self, queued: int):
        if time.monotonic() - self.last_report < self.interval_s:
            return
        self.last_report = time.monotonic()
        latency = "n=0"
        if len(self.latencies) >= 2:
            cuts = statistics.quantiles(self.latencies, n=20, method="inclusive")
            latency = f"n={len(self.latencies)} p50={cuts[9]:.3f}s p95={cuts[18]:.3f}s"
        log.info("stats %s queued=%d source_to_producer %s", dict(self.counts), queued, latency)
        self.latencies.clear()


def make_producer(cfg: Config) -> Producer:
    def on_error(err):
        log.warning("kafka client error: %s", err)
        if err.fatal():
            raise SystemExit(f"fatal kafka error: {err}")

    return Producer({
        "bootstrap.servers": cfg.kafka_bootstrap,
        "client.id": "wikipedia-sse-producer",
        "enable.idempotence": True,  # implies acks=all and safe retries
        "compression.type": "lz4",
        "linger.ms": 50,
        # Keep retrying during a broker outage. The bounded queue then fills, produce()
        # blocks, and SSE resumes later from Last-Event-ID instead of dropping events.
        "message.timeout.ms": 0,
        "queue.buffering.max.messages": 20000,
        "error_cb": on_error,
    })


def on_delivery(stats: Stats):
    def callback(err, msg):
        if err is None:
            stats.counts["delivered"] += 1
        else:
            stats.counts["delivery_failed"] += 1
            log.error("delivery failed key=%s: %s", msg.key(), err)
    return callback


def publish(producer, topic: str, key, envelope: dict, stats: Stats):
    value = json.dumps(envelope, ensure_ascii=False).encode("utf-8")
    while True:
        try:
            producer.produce(topic, key=key, value=value, callback=on_delivery(stats))
            break
        except BufferError:  # queue full: wait for the broker, which also stops reading SSE
            stats.counts["queue_full_waits"] += 1
            producer.poll(1)
    producer.poll(0)


def handle_event(cfg, producer, stats, event_type: str, data: str):
    if event_type != "message":
        stats.counts["sse_error_events"] += 1
        log.warning("SSE %s event: %s", event_type, data[:500])
        return
    stats.counts["received"] += 1
    received_at = datetime.now(timezone.utc)
    try:
        event = parse_event(data)
    except ValueError as exc:
        stats.counts["malformed"] += 1
        log.warning("malformed event (%s): %s", exc, data[:200])
        return
    # Wikimedia injects monitoring events that are not real changes.
    if event_meta(event).get("domain") == "canary":
        stats.counts["canary_skipped"] += 1
        return
    key, envelope = build_message(event, data, received_at)
    publish(producer, cfg.kafka_topic, key, envelope, stats)
    stats.observe(envelope, received_at)


def open_stream(cfg: Config, last_event_id):
    headers = {"Accept": "text/event-stream", "User-Agent": cfg.user_agent}
    if last_event_id:
        headers["Last-Event-ID"] = last_event_id
    request = urllib.request.Request(cfg.sse_url, headers=headers)
    return urllib.request.urlopen(request, timeout=cfg.read_timeout_s)


def retry_after_seconds(exc: urllib.error.HTTPError):
    try:
        return float(exc.headers.get("Retry-After"))
    except (TypeError, ValueError):
        return None


def run(cfg, producer, stats, open_stream=open_stream, sleep=time.sleep):
    """Consume SSE until stopped by a signal; reconnect on any stream failure.

    shortcut: the resume position lives in memory, so events during a producer restart
    are not recovered (the gap is visible in the logs); persist last_id to close that.
    """
    last_id = None
    backoff = cfg.backoff_min_s
    down_since = None
    while True:
        wait = None
        try:
            with open_stream(cfg, last_id) as stream:
                log.info("connected to %s last_event_id=%s", cfg.sse_url, bool(last_id))
                for event_type, event_id, data in iter_sse(stream):
                    if down_since is not None:
                        gap = (datetime.now(timezone.utc) - down_since).total_seconds()
                        log.warning("SSE gap closed gap_s=%.1f resumed_from_last_id=%s",
                                    gap, bool(last_id))
                        down_since = None
                    handle_event(cfg, producer, stats, event_type, data)
                    # Advance only after Kafka accepted the event, so a resume replays it.
                    last_id = event_id or last_id
                    backoff = cfg.backoff_min_s
                    stats.report_if_due(len(producer))
            reason = "server closed the stream"
        except urllib.error.HTTPError as exc:
            reason = f"HTTP {exc.code}"
            wait = retry_after_seconds(exc)
        except (OSError, http.client.HTTPException) as exc:
            reason = repr(exc)
        producer.poll(0)
        stats.counts["reconnects"] += 1
        down_since = down_since or datetime.now(timezone.utc)
        wait = wait if wait is not None else backoff * random.uniform(0.5, 1.0)
        log.warning("SSE disconnected at %s: %s; retry in %.1fs",
                    to_utc_iso(down_since), reason, wait)
        sleep(wait)
        backoff = min(backoff * 2, cfg.backoff_max_s)


def main():
    logging.basicConfig(level=logging.INFO, stream=sys.stdout,
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    cfg = Config.from_env()
    stats = Stats(cfg.stats_interval_s)
    producer = make_producer(cfg)

    def stop(signum, _frame):
        log.info("stopping on signal %s", signum)
        raise SystemExit(0)

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    log.info("start topic=%s bootstrap=%s", cfg.kafka_topic, cfg.kafka_bootstrap)
    try:
        run(cfg, producer, stats)
    finally:
        left = producer.flush(30)
        log.info("shutdown final_counts=%s undelivered=%d", dict(stats.counts), left)


if __name__ == "__main__":
    main()
