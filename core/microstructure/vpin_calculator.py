"""Event-driven Volume-Synchronized Probability of Informed Trading (VPIN)."""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class VPINConfig:
    volume_bucket_size: float
    rolling_buckets: int = 50
    rank_window: int = 200
    min_rank_observations: int = 20
    volume_tolerance: float = 1e-10

    def __post_init__(self):
        if self.volume_bucket_size <= 0 or self.rolling_buckets < 1:
            raise ValueError("bucket size and rolling_buckets must be positive")
        if self.rank_window < 1 or self.min_rank_observations < 1:
            raise ValueError("rank settings must be positive")


@dataclass(frozen=True)
class VPINBucket:
    index: int
    buy_volume: float
    sell_volume: float
    total_volume: float
    imbalance: float
    vpin: float


@dataclass(frozen=True)
class VPINResult:
    timestamp: object
    current_vpin: Optional[float]
    rolling_cdf_toxicity_rank: Optional[float]
    bucket_fill_percentage: float
    current_bucket_buy: float
    current_bucket_sell: float
    current_bucket_imbalance: float
    current_bucket_total: float
    completed_buckets: int
    latest_completed_bucket: Optional[VPINBucket]


class VPINCalculator:
    def __init__(self, config: VPINConfig):
        self.config = config
        self._buy = 0.0
        self._sell = 0.0
        self._completed = deque(maxlen=config.rolling_buckets)
        self._history = deque(maxlen=config.rank_window)
        self._last_price = None
        self._last_side = None
        self._index = 0
        self._timestamp = None

    def _classify(self, side, price):
        if side is not None:
            normalized = str(side).upper()
            if normalized not in ("BUY", "SELL"):
                raise ValueError("side must be BUY or SELL")
            return normalized
        if price is None:
            raise ValueError("first trade needs explicit BUY/SELL side or a price and prior price")
        if self._last_price is None:
            raise ValueError("first trade needs explicit BUY/SELL side")
        if price > self._last_price:
            return "BUY"
        if price < self._last_price:
            return "SELL"
        if self._last_side is None:
            raise ValueError("tick-rule fallback has no prior classified side")
        return self._last_side

    def update(self, timestamp, quantity: float, side: Optional[str] = None,
               price: Optional[float] = None) -> VPINResult:
        if quantity <= 0:
            raise ValueError("quantity must be positive")
        if price is not None and price <= 0:
            raise ValueError("price must be positive")
        classified = self._classify(side, price)
        remaining = float(quantity)
        bucket_size = self.config.volume_bucket_size
        tol = self.config.volume_tolerance
        while remaining > tol:
            used = self._buy + self._sell
            capacity = bucket_size - used
            take = min(remaining, capacity)
            if classified == "BUY":
                self._buy += take
            else:
                self._sell += take
            remaining -= take
            if bucket_size - (self._buy + self._sell) <= tol:
                total = self._buy + self._sell
                imbalance = abs(self._buy - self._sell)
                bucket = VPINBucket(self._index, self._buy, self._sell,
                                    total, imbalance, imbalance / bucket_size)
                self._completed.append(bucket)
                self._history.append(bucket.vpin)
                self._index += 1
                self._buy = self._sell = 0.0
        self._last_side = classified
        if price is not None:
            self._last_price = float(price)
        self._timestamp = timestamp
        current = (sum(b.vpin for b in self._completed) / len(self._completed)
                   if self._completed else None)
        rank = None
        if len(self._history) >= self.config.min_rank_observations:
            # Empirical percentile of current rolling VPIN against completed bucket history.
            rank = sum(1 for value in self._history if value <= current) / len(self._history)
        fill = (self._buy + self._sell) / bucket_size * 100.0
        latest = self._completed[-1] if self._completed else None
        return VPINResult(timestamp, current, rank, fill, self._buy, self._sell,
                          abs(self._buy - self._sell), self._buy + self._sell,
                          self._index, latest)
