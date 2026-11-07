"""Producer settings, read from environment variables (see .env.example)."""
import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    sse_url: str = "https://stream.wikimedia.org/v2/stream/recentchange"
    # Wikimedia's policy requires a descriptive User-Agent; generic clients get blocked.
    user_agent: str = "wikipedia-realtime-analytics/0.1 (https://github.com/noway165/BigData)"
    kafka_bootstrap: str = "localhost:9094"
    kafka_topic: str = "wikipedia-recent-changes"
    read_timeout_s: float = 60.0
    backoff_min_s: float = 1.0
    backoff_max_s: float = 60.0
    stats_interval_s: float = 30.0

    @classmethod
    def from_env(cls) -> "Config":
        env = os.environ.get
        return cls(
            sse_url=env("WIKI_SSE_URL", cls.sse_url),
            user_agent=env("WIKI_USER_AGENT", cls.user_agent),
            kafka_bootstrap=env("KAFKA_BOOTSTRAP_SERVERS", cls.kafka_bootstrap),
            kafka_topic=env("KAFKA_TOPIC", cls.kafka_topic),
            read_timeout_s=float(env("SSE_READ_TIMEOUT_S", cls.read_timeout_s)),
            backoff_min_s=float(env("SSE_BACKOFF_MIN_S", cls.backoff_min_s)),
            backoff_max_s=float(env("SSE_BACKOFF_MAX_S", cls.backoff_max_s)),
            stats_interval_s=float(env("STATS_INTERVAL_S", cls.stats_interval_s)),
        )
