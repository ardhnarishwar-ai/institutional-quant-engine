"""Rolling tick-CVD/price-displacement absorption candidate detector.

This detects a candidate condition, not hidden iceberg identity. Aggressor side
must be classified upstream from exchange trade/quote data.
"""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class AbsorptionConfig:
    window_ticks: int = 100
    min_volume: float = 1.0
    max_abs_price_change_bps: float = 2.0
    min_delta_ratio: float = 0.65
    level_tolerance_bps: float = 3.0

    def __post_init__(self):
        if self.window_ticks < 2 or self.min_volume < 0:
            raise ValueError("invalid window_ticks or min_volume")
        if self.max_abs_price_change_bps < 0 or not 0 <= self.min_delta_ratio <= 1:
            raise ValueError("invalid absorption thresholds")


@dataclass(frozen=True)
class AbsorptionSignal:
    timestamp: object
    price: float
    reference_level: Optional[float]
    cvd_delta: float
    buy_volume: float
    sell_volume: float
    price_change_bps: float
    delta_ratio: float
    signal: str
    absorbing_side: Optional[str]


class AbsorptionDetector:
    def __init__(self, config: AbsorptionConfig = AbsorptionConfig()):
        self.config = config
        self._ticks = deque(maxlen=config.window_ticks)

    def update(self, timestamp, price: float, quantity: float,
               aggressor_side: str, reference_level: Optional[float] = None):
        side = str(aggressor_side).upper()
        if side not in ("BUY", "SELL"):
            raise ValueError("aggressor_side must be BUY or SELL")
        if price <= 0 or quantity <= 0:
            raise ValueError("price and quantity must be positive")
        if reference_level is not None and reference_level <= 0:
            raise ValueError("reference_level must be positive")
        self._ticks.append((timestamp, float(price), float(quantity), side, reference_level))
        return self.snapshot()

    def snapshot(self) -> AbsorptionSignal:
        if not self._ticks:
            return AbsorptionSignal(None, 0.0, None, 0, 0, 0, 0, 0, "INSUFFICIENT_DATA", None)
        first, last = self._ticks[0], self._ticks[-1]
        buy = sum(t[2] for t in self._ticks if t[3] == "BUY")
        sell = sum(t[2] for t in self._ticks if t[3] == "SELL")
        total = buy + sell
        delta = buy - sell
        displacement = (last[1] / first[1] - 1.0) * 10000.0
        ratio = abs(delta) / total if total else 0.0
        level = last[4]
        near_level = level is not None and abs(last[1] / level - 1.0) * 10000 <= self.config.level_tolerance_bps
        signal, absorbing = "NONE", None
        if len(self._ticks) >= 2 and total >= self.config.min_volume and near_level:
            if abs(displacement) <= self.config.max_abs_price_change_bps and ratio >= self.config.min_delta_ratio:
                if delta > 0:
                    signal, absorbing = "BUY_ABSORPTION_CANDIDATE", "SELL"
                elif delta < 0:
                    signal, absorbing = "SELL_ABSORPTION_CANDIDATE", "BUY"
        return AbsorptionSignal(last[0], last[1], level, delta, buy, sell,
                                displacement, ratio, signal, absorbing)
