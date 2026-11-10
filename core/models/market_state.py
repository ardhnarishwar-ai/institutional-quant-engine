"""Thread-safe state aggregation with per-feed sequence and event-time checks.

Sequence numbers are compared only within a shared sequence_domain. Independent
exchange feeds commonly have unrelated sequence spaces and must not be compared.
Each component's own sequence must be strictly increasing when supplied.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from threading import RLock
from typing import Any, Optional


@dataclass(frozen=True)
class MarketStateConfig:
    max_component_age_ms: int = 3000
    max_cross_feed_skew_ms: int = 1000
    max_cross_feed_sequence_skew: int = 0

    def __post_init__(self):
        if min(self.max_component_age_ms, self.max_cross_feed_skew_ms,
               self.max_cross_feed_sequence_skew) < 0:
            raise ValueError("consistency limits cannot be negative")


@dataclass(frozen=True)
class MarketState:
    timestamp: str
    gex: Any = None
    vpin: Any = None
    absorption: Any = None
    orderbook: Any = None
    component_timestamps_ms: dict = field(default_factory=dict)
    component_sequences: dict = field(default_factory=dict)
    component_sequence_domains: dict = field(default_factory=dict)
    stale_components: tuple = ()
    cross_feed_skew_ms: Optional[int] = None
    cross_feed_sequence_skew: Optional[int] = None
    flags: tuple = ()


class MarketStateAggregator:
    COMPONENTS = ("gex", "vpin", "absorption", "orderbook")

    def __init__(self, config: MarketStateConfig = MarketStateConfig()):
        self.config = config
        self._lock = RLock()
        self._values = {name: None for name in self.COMPONENTS}
        self._timestamps = {name: None for name in self.COMPONENTS}
        self._sequences = {name: None for name in self.COMPONENTS}
        self._domains = {name: None for name in self.COMPONENTS}

    def update(self, component: str, value: Any, timestamp_ms: int,
               sequence_number: Optional[int] = None,
               sequence_domain: Optional[str] = None):
        if component not in self.COMPONENTS:
            raise ValueError(f"unknown component: {component}")
        if not isinstance(timestamp_ms, int) or timestamp_ms < 0:
            raise ValueError("timestamp_ms must be a non-negative integer")
        if sequence_number is None and sequence_domain is not None:
            raise ValueError("sequence_domain requires sequence_number")
        if sequence_number is not None:
            if not isinstance(sequence_number, int) or sequence_number < 0:
                raise ValueError("sequence_number must be a non-negative integer")
            if not sequence_domain or not str(sequence_domain).strip():
                raise ValueError("sequence_number requires a non-empty sequence_domain")
        with self._lock:
            previous_ts = self._timestamps[component]
            previous_seq = self._sequences[component]
            if previous_ts is not None and timestamp_ms < previous_ts:
                raise ValueError("component timestamp moved backwards")
            if sequence_number is not None:
                if previous_seq is not None:
                    if sequence_domain != self._domains[component]:
                        raise ValueError("sequence domain cannot change within a component stream")
                    if sequence_number <= previous_seq:
                        raise ValueError("component sequence must increase strictly")
            self._values[component] = value
            self._timestamps[component] = timestamp_ms
            self._sequences[component] = sequence_number
            self._domains[component] = sequence_domain

    def snapshot(self, now_ms: int):
        if not isinstance(now_ms, int) or now_ms < 0:
            raise ValueError("now_ms must be a non-negative integer")
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

            seq_pairs = [(self._domains[k], self._sequences[k])
                         for k in self.COMPONENTS
                         if self._sequences[k] is not None]
            domains = {d for d, _ in seq_pairs}
            sequence_skew = None
            if len(domains) == 1 and len(seq_pairs) >= 2:
                seqs = [s for _, s in seq_pairs]
                sequence_skew = max(seqs) - min(seqs)
                if sequence_skew > self.config.max_cross_feed_sequence_skew:
                    flags.append("CROSS_FEED_SEQUENCE_SKEW")
            elif len(domains) > 1:
                # Sequence values from distinct domains are intentionally incomparable.
                flags.append("MIXED_SEQUENCE_DOMAINS")
            if any(self._sequences[k] is None for k in self.COMPONENTS
                   if self._timestamps[k] is not None):
                flags.append("SEQUENCE_METADATA_MISSING")

            values = {k: (None if k in stale else self._values[k])
                      for k in self.COMPONENTS}
            return MarketState(
                datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc).isoformat(),
                **values,
                component_timestamps_ms=dict(self._timestamps),
                component_sequences=dict(self._sequences),
                component_sequence_domains=dict(self._domains),
                stale_components=stale,
                cross_feed_skew_ms=skew,
                cross_feed_sequence_skew=sequence_skew,
                flags=tuple(flags),
            )

    @staticmethod
    def to_dict(state: MarketState):
        return asdict(state)
