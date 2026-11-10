"""Deterministic, assumption-explicit option gamma exposure (GEX).

GEX is conditional on dealer_position_sign supplied for every option row.
Open interest alone does not identify dealer inventory or dealer gamma sign.
All strike exposures use the same 1%-spot-move convention.

For deterministic replay, pass as_of_timestamp_ns from the canonical event
clock. If omitted, an explicit aware/naive datetime `as_of` may be supplied;
only when both are omitted does the calculator use wall-clock UTC for legacy
interactive use. Replay callers should never rely on that fallback.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from math import erf, exp, isfinite, log, pi, sqrt
from typing import Iterable, Mapping, Optional


@dataclass(frozen=True)
class GEXResult:
    spot: float
    net_gex: float
    call_wall: Optional[float]
    put_wall: Optional[float]
    zero_flip: Optional[float]
    zero_crossings: tuple[float, ...]
    call_gex: Mapping[float, float]
    put_gex: Mapping[float, float]
    unit: str = "currency_per_1pct_spot_move"
    as_of_timestamp_ns: Optional[int] = None
    as_of_iso: Optional[str] = None
    dealer_positioning_assumption: str = "per-row dealer_position_sign; conditional, not inferred from OI"


class GEXCalculator:
    """Black-Scholes gamma and signed OI-weighted GEX by strike.

    Required row fields: strike, option_type ('C'/'P'), expiry (datetime or
    ISO-8601 string), open_interest, iv (annualized decimal),
    contract_multiplier, dealer_position_sign (+1 long / -1 short dealer gamma).
    """

    @staticmethod
    def _finite(value, name: str) -> float:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise TypeError(f"{name} must be numeric")
        value = float(value)
        if not isfinite(value):
            raise ValueError(f"{name} must be finite")
        return value

    @classmethod
    def _positive(cls, value, name: str) -> float:
        value = cls._finite(value, name)
        if value <= 0:
            raise ValueError(f"{name} must be positive")
        return value

    @staticmethod
    def _normal_pdf(x: float) -> float:
        return exp(-0.5 * x * x) / sqrt(2.0 * pi)

    @classmethod
    def bs_gamma(cls, spot: float, strike: float, years: float, iv: float,
                 rate: float = 0.0, dividend_yield: float = 0.0) -> float:
        spot = cls._positive(spot, "spot")
        strike = cls._positive(strike, "strike")
        years = cls._finite(years, "years")
        iv = cls._finite(iv, "iv")
        rate = cls._finite(rate, "rate")
        dividend_yield = cls._finite(dividend_yield, "dividend_yield")
        if years <= 0 or iv <= 0:
            return 0.0
        d1 = (log(spot / strike) + (rate - dividend_yield + 0.5 * iv * iv) * years) / (iv * sqrt(years))
        return exp(-dividend_yield * years) * cls._normal_pdf(d1) / (spot * iv * sqrt(years))

    @staticmethod
    def _expiry_ns(value) -> int:
        if isinstance(value, str):
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if not isinstance(value, datetime):
            raise TypeError("expiry must be datetime or ISO-8601 string")
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        else:
            value = value.astimezone(timezone.utc)
        return int(value.timestamp() * 1_000_000_000)

    @staticmethod
    def _as_of(as_of: Optional[datetime], as_of_timestamp_ns: Optional[int]):
        if as_of_timestamp_ns is not None:
            if not isinstance(as_of_timestamp_ns, int) or isinstance(as_of_timestamp_ns, bool) or as_of_timestamp_ns < 0:
                raise ValueError("as_of_timestamp_ns must be a non-negative integer")
            if as_of is not None:
                raise ValueError("provide as_of or as_of_timestamp_ns, not both")
            seconds, nanos = divmod(as_of_timestamp_ns, 1_000_000_000)
            dt = datetime.fromtimestamp(seconds, tz=timezone.utc).replace(
                microsecond=nanos // 1000
            )
            return as_of_timestamp_ns, dt
        if as_of is None:
            dt = datetime.now(timezone.utc)
        elif not isinstance(as_of, datetime):
            raise TypeError("as_of must be datetime")
        elif as_of.tzinfo is None:
            dt = as_of.replace(tzinfo=timezone.utc)
        else:
            dt = as_of.astimezone(timezone.utc)
        ns = int(dt.timestamp() * 1_000_000_000)
        return ns, dt

    def calculate(
        self, options: Iterable[Mapping], spot: float,
        as_of: Optional[datetime] = None, rate: float = 0.0,
        dividend_yield: float = 0.0, grid_points: int = 401,
        grid_width: float = 0.20, *,
        as_of_timestamp_ns: Optional[int] = None,
    ) -> GEXResult:
        spot = self._positive(spot, "spot")
        rate = self._finite(rate, "rate")
        dividend_yield = self._finite(dividend_yield, "dividend_yield")
        if not isinstance(grid_points, int) or isinstance(grid_points, bool) or grid_points < 3:
            raise ValueError("grid_points must be an integer >= 3")
        grid_width = self._positive(grid_width, "grid_width")
        asof_ns, asof_dt = self._as_of(as_of, as_of_timestamp_ns)
        rows = list(options)
        normalized = []
        for index, row in enumerate(rows):
            required = ("strike", "option_type", "expiry", "open_interest", "iv",
                        "contract_multiplier", "dealer_position_sign")
            missing = [key for key in required if key not in row]
            if missing:
                raise ValueError(f"option row {index} missing fields: {', '.join(missing)}")
            strike = self._positive(row["strike"], "strike")
            oi = self._finite(row["open_interest"], "open_interest")
            iv = self._finite(row["iv"], "iv")
            multiplier = self._positive(row["contract_multiplier"], "contract_multiplier")
            sign = self._finite(row["dealer_position_sign"], "dealer_position_sign")
            typ = str(row["option_type"]).upper()
            if typ not in ("C", "P"):
                raise ValueError("option_type must be C or P")
            if oi < 0 or iv < 0:
                raise ValueError("open_interest and iv cannot be negative")
            if sign not in (-1.0, 1.0):
                raise ValueError("dealer_position_sign must be explicitly +1 or -1")
            expiry_ns = self._expiry_ns(row["expiry"])
            years = max(0.0, (expiry_ns - asof_ns) / (365.0 * 86400.0 * 1_000_000_000))
            normalized.append((strike, typ, oi, iv, multiplier, sign, expiry_ns, years))

        call: dict[float, float] = {}
        put: dict[float, float] = {}

        def exposure_at(test_spot: float, item) -> float:
            strike, typ, oi, iv, multiplier, sign, expiry_ns, years = item
            gamma = self.bs_gamma(test_spot, strike, years, iv, rate, dividend_yield)
            return sign * gamma * oi * multiplier * test_spot * test_spot * 0.01

        for item in normalized:
            value = exposure_at(spot, item)
            bucket = call if item[1] == "C" else put
            bucket[item[0]] = bucket.get(item[0], 0.0) + value

        if not normalized:
            return GEXResult(
                spot, 0.0, None, None, None, (), {}, {},
                as_of_timestamp_ns=asof_ns, as_of_iso=asof_dt.isoformat(),
            )

        call_wall = max(call, key=lambda k: (abs(call[k]), -k)) if call else None
        put_wall = max(put, key=lambda k: (abs(put[k]), -k)) if put else None

        def net_at(test_spot: float) -> float:
            return sum(exposure_at(test_spot, item) for item in normalized)

        lo, hi = spot * (1.0 - grid_width), spot * (1.0 + grid_width)
        xs = [lo + (hi - lo) * i / (grid_points - 1) for i in range(grid_points)]
        ys = [net_at(x) for x in xs]
        crossings = []
        for i in range(len(xs) - 1):
            if ys[i] == 0.0:
                crossings.append(xs[i])
            elif ys[i] * ys[i + 1] < 0.0:
                crossings.append(xs[i] - ys[i] * (xs[i + 1] - xs[i]) / (ys[i + 1] - ys[i]))
        # De-duplicate exact grid zeros shared by adjacent intervals.
        unique_crossings = tuple(sorted({round(x, 10) for x in crossings}))
        zero = min(unique_crossings, key=lambda x: (abs(x - spot), x)) if unique_crossings else None
        return GEXResult(
            spot=spot, net_gex=sum(call.values()) + sum(put.values()),
            call_wall=call_wall, put_wall=put_wall, zero_flip=zero,
            zero_crossings=unique_crossings, call_gex=dict(sorted(call.items())),
            put_gex=dict(sorted(put.items())),
            as_of_timestamp_ns=asof_ns, as_of_iso=asof_dt.isoformat(),
        )
