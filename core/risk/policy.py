"""Instrument-isolated risk policies and explicit scoring weights."""
from __future__ import annotations
from dataclasses import dataclass
from typing import Mapping, Tuple
import math


@dataclass(frozen=True, slots=True)
class RiskPolicy:
    instrument_id: str
    policy_version: str = "risk-v1"
    weights: Tuple[Tuple[str, float], ...] = (
        ("market", 0.25), ("liquidity", 0.25), ("vpin", 0.20),
        ("gex", 0.10), ("exposure", 0.20),
    )
    max_signal_age_ns: int = 2_000_000_000
    max_feed_age_ns: int = 2_000_000_000
    max_gex_age_ns: int = 60_000_000_000
    max_spread_ticks: float = 4.0
    min_depth_units: float = 1.0
    max_exposure_utilization: float = 0.80
    max_daily_loss_utilization: float = 0.80
    hold_risk_score: float = 0.60
    reject_risk_score: float = 0.80
    critical_vpin: float = 0.95

    def __post_init__(self):
        if not self.instrument_id or not self.policy_version:
            raise ValueError("instrument_id and policy_version required")
        names = [k for k, _ in self.weights]
        if len(names) != len(set(names)) or set(names) != {"market", "liquidity", "vpin", "gex", "exposure"}:
            raise ValueError("weights must define five unique risk dimensions")
        if any(isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v) or v < 0 for _, v in self.weights):
            raise ValueError("weights must be finite and non-negative")
        if not math.isclose(sum(v for _, v in self.weights), 1.0, abs_tol=1e-9):
            raise ValueError("risk weights must sum to 1")
        for name in ("max_signal_age_ns", "max_feed_age_ns", "max_gex_age_ns"):
            v = getattr(self, name)
            if not isinstance(v, int) or isinstance(v, bool) or v <= 0:
                raise ValueError(f"{name} must be positive integer")
        for name in ("max_spread_ticks", "min_depth_units", "max_exposure_utilization", "max_daily_loss_utilization", "hold_risk_score", "reject_risk_score", "critical_vpin"):
            v = getattr(self, name)
            if isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v) or not 0 <= v <= 1 and name not in ("max_spread_ticks", "min_depth_units"):
                raise ValueError(f"{name} outside allowed range")
        if self.max_spread_ticks <= 0 or self.min_depth_units <= 0:
            raise ValueError("spread and depth limits must be positive")
        if not 0 <= self.hold_risk_score < self.reject_risk_score <= 1:
            raise ValueError("risk thresholds must satisfy hold < reject")


DEFAULT_RISK_POLICIES: Mapping[str, RiskPolicy] = {
    "NSE:NSE_SPOT:NIFTY:INR": RiskPolicy("NSE:NSE_SPOT:NIFTY:INR"),
    "NSE:NSE_SPOT:BANKNIFTY:INR": RiskPolicy("NSE:NSE_SPOT:BANKNIFTY:INR"),
    "NSE:NSE_SPOT:FINNIFTY:INR": RiskPolicy("NSE:NSE_SPOT:FINNIFTY:INR"),
    "BINANCE:CRYPTO_PERPETUAL:BTCUSDT:USDT": RiskPolicy(
        "BINANCE:CRYPTO_PERPETUAL:BTCUSDT:USDT",
        max_gex_age_ns=300_000_000_000,
        max_spread_ticks=8.0,
    ),
}
