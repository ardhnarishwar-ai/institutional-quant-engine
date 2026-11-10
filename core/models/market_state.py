"""Thread-safe aggregation of independently timestamped market observations."""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from threading import RLock
from typing import Any, Optional


@dataclass(frozen=True)
class MarketStateConfig:
    max_component_age_ms: int = 3000
    max_cross_feed_skew_ms: int = 1000

    def __post_init__(self):
        if self.max_component_age_ms < 0 or self.max_cross_feed_skew_ms < 0:
            raise ValueError("staleness limits cannot be negative")


@dataclass(frozen=True)
class MarketState:
    timestamp: str
    gex: Any = None
    vpin: Any = None
    absorption: Any = None
    orderbook: Any = None
    component_timestamps_ms: dict = field(default_factory=dict)
    stale_components: tuple = ()
    cross_feed_skew_ms: Optional[int] = None
    flags: tuple = ()


class MarketStateAggregator:
    COMPONENTS = ("gex", "vpin", "absorption", "orderbook")

    def __init__(self, config: MarketStateConfig = MarketStateConfig()):
        self.config = config
        self._lock = RLock()
        self._values = {name: None for name in self.COMPONENTS}
        self._timestamps = {name: None for name in self.COMPONENTS}

    def update(self, component: str, value: Any, timestamp_ms: int):
        if component not in self.COMPONENTS:
            raise ValueError(f"unknown component: {component}")
        if timestamp_ms < 0:
            raise ValueError("timestamp_ms cannot be negative")
        with self._lock:
            previous = self._timestamps[component]
            if previous is not None and timestamp_ms < previous:
                raise ValueError("component timestamp moved backwards")
            self._values[component] = value
            self._timestamps[component] = timestamp_ms

    def snapshot(self, now_ms: int):
        if now_ms < 0:
            raise ValueError("now_ms cannot be negative")
        with self._lock:
            ages = {k: (now_ms - ts if ts is not None else None)
                    for k, ts in self._timestamps.items()}
            stale = tuple(k for k, age in ages.items()
                          if age is None or age > self.config.max_component_age_ms)
            present_ts = [ts for ts in self._timestamps.values() if ts is not None]
            skew = max(present_ts) - min(present_ts) if len(present_ts) >= 2 else None
            flags = []
            if stale:
                flags.append("STALE_COMPONENTS")
            if skew is not None and skew > self.config.max_cross_feed_skew_ms:
                flags.append("CROSS_FEED_SKEW")
            values = {k: (None if k in stale else self._values[k])
                      for k in self.COMPONENTS}
            return MarketState(
                datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc).isoformat(),
                **values,
                component_timestamps_ms=dict(self._timestamps),
                stale_components=stale,
                cross_feed_skew_ms=skew,
                flags=tuple(flags),
            )

    @staticmethod
    def to_dict(state: MarketState):
        return asdict(state)
