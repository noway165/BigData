"""Sample the live Wikimedia stream and report which fields events really contain.

Usage: python scripts/inspect_sse_schema.py [seconds] [sample_output.jsonl]
Reuses the producer's SSE code, so install src/producer/requirements.txt first.
"""
import json
import sys
import time
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src" / "producer"))

from config import Config  # noqa: E402
from sse_kafka_producer import iter_sse, open_stream  # noqa: E402


def field_presence(events):
    """Count, per field, how many events have it, and how many have it as null."""
    present, nulls = Counter(), Counter()
    for event in events:
        flat = dict(event)
        flat.update({f"meta.{k}": v for k, v in (event.get("meta") or {}).items()})
        for name, value in flat.items():
            present[name] += 1
            nulls[name] += value is None
    return present, nulls


def main():
    seconds = float(sys.argv[1]) if len(sys.argv) > 1 else 30
    sample_path = sys.argv[2] if len(sys.argv) > 2 else None
    events, deadline = [], time.monotonic() + seconds
    with open_stream(Config.from_env(), None) as stream:
        for event_type, _event_id, data in iter_sse(stream):
            if event_type == "message":
                events.append(json.loads(data))
            if time.monotonic() >= deadline:
                break

    present, nulls = field_presence(events)
    print(f"events: {len(events)} in {seconds:.0f}s")
    print(f"type: {dict(Counter(e.get('type') for e in events))}")
    print(f"bot: {dict(Counter(repr(e.get('bot', 'MISSING')) for e in events))}")
    print(f"{'field':<24}{'present':>9}{'missing':>9}{'null':>7}")
    for name, count in present.most_common():
        print(f"{name:<24}{count:>9}{len(events) - count:>9}{nulls[name]:>7}")
    if sample_path:
        with open(sample_path, "w", encoding="utf-8", newline="\n") as out:
            out.writelines(json.dumps(e, ensure_ascii=False) + "\n" for e in events)


if __name__ == "__main__":
    main()
