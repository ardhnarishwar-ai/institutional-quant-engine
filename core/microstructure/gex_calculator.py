"""Transparent option gamma-exposure calculations.

Important: public open interest does not reveal which side dealers hold.
Each row therefore requires an explicit dealer_position_sign supplied by an
external positioning model/source. Results are conditional on those signs.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from math import erf, exp, log, pi, sqrt
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


class GEXCalculator:
    """Black-Scholes gamma and signed OI-weighted GEX.

    Required option row fields:
      strike, option_type ('C'/'P'), expiry (datetime or ISO string),
      open_interest, iv (annualized decimal), contract_multiplier,
      dealer_position_sign (+1 dealer long gamma, -1 dealer short gamma).
    """

    @staticmethod
    def _normal_pdf(x: float) -> float:
        return exp(-0.5 * x * x) / sqrt(2.0 * pi)

    @staticmethod
    def _normal_cdf(x: float) -> float:
        return 0.5 * (1.0 + erf(x / sqrt(2.0)))

    @classmethod
    def bs_gamma(
        cls, spot: float, strike: float, years: float, iv: float,
        rate: float = 0.0, dividend_yield: float = 0.0,
    ) -> float:
        if spot <= 0 or strike <= 0:
            raise ValueError("spot and strike must be positive")
        if years <= 0 or iv <= 0:
            return 0.0
        d1 = (log(spot / strike) + (rate - dividend_yield + 0.5 * iv * iv) * years) / (iv * sqrt(years))
        return exp(-dividend_yield * years) * cls._normal_pdf(d1) / (spot * iv * sqrt(years))

    @staticmethod
    def _expiry_datetime(value) -> datetime:
        if isinstance(value, datetime):
            return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        if isinstance(value, str):
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
        raise TypeError("expiry must be datetime or ISO-8601 string")

    def calculate(
        self, options: Iterable[Mapping], spot: float,
        as_of: Optional[datetime] = None, rate: float = 0.0,
        dividend_yield: float = 0.0, grid_points: int = 401,
        grid_width: float = 0.20,
    ) -> GEXResult:
        if spot <= 0:
            raise ValueError("spot must be positive")
        if grid_points < 3 or grid_width <= 0:
            raise ValueError("grid_points must be >= 3 and grid_width positive")
        now = as_of or datetime.now(timezone.utc)
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)

        call: dict[float, float] = {}
        put: dict[float, float] = {}
        rows = list(options)
        for row in rows:
            required = ("strike", "option_type", "expiry", "open_interest", "iv",
                        "contract_multiplier", "dealer_position_sign")
            missing = [key for key in required if key not in row]
            if missing:
                raise ValueError(f"missing option fields: {', '.join(missing)}")
            strike = float(row["strike"])
            oi = float(row["open_interest"])
            iv = float(row["iv"])
            multiplier = float(row["contract_multiplier"])
            sign = float(row["dealer_position_sign"])
            typ = str(row["option_type"]).upper()
            if typ not in ("C", "P"):
                raise ValueError("option_type must be C or P")
            if strike <= 0 or oi < 0 or iv < 0 or multiplier <= 0:
                raise ValueError("invalid strike, OI, IV, or multiplier")
            if sign not in (-1.0, 1.0):
                raise ValueError("dealer_position_sign must be +1 or -1")
            expiry = self._expiry_datetime(row["expiry"])
            years = max(0.0, (expiry - now).total_seconds() / (365.0 * 86400.0))
            gamma = self.bs_gamma(spot, strike, years, iv, rate, dividend_yield)
            # Standard 1%-move convention; units are currency, not contracts.
            exposure = sign * gamma * oi * multiplier * spot * spot * 0.01
            bucket = call if typ == "C" else put
            bucket[strike] = bucket.get(strike, 0.0) + exposure

        strikes = sorted(set(call) | set(put))
        if not strikes:
            return GEXResult(spot, 0.0, None, None, None, (), {}, {})
        call_wall = max(call, key=lambda k: abs(call[k])) if call else None
        put_wall = max(put, key=lambda k: abs(put[k])) if put else None

        def net_at(test_spot: float) -> float:
            total = 0.0
            for row in rows:
                expiry = self._expiry_datetime(row["expiry"])
                years = max(0.0, (expiry - now).total_seconds() / (365.0 * 86400.0))
                gamma = self.bs_gamma(test_spot, float(row["strike"]), years,
                                      float(row["iv"]), rate, dividend_yield)
                total += (float(row["dealer_position_sign"]) * gamma *
                          float(row["open_interest"]) * float(row["contract_multiplier"]) *
                          test_spot * test_spot * 0.01)
            return total

        lo, hi = spot * (1.0 - grid_width), spot * (1.0 + grid_width)
        xs = [lo + (hi - lo) * i / (grid_points - 1) for i in range(grid_points)]
        ys = [net_at(x) for x in xs]
        crossings = []
        for i in range(len(xs) - 1):
            if ys[i] == 0:
                crossings.append(xs[i])
            elif ys[i] * ys[i + 1] < 0:
                crossings.append(xs[i] - ys[i] * (xs[i + 1] - xs[i]) / (ys[i + 1] - ys[i]))
        zero = min(crossings, key=lambda x: abs(x - spot)) if crossings else None
        return GEXResult(spot, sum(call.values()) + sum(put.values()),
                         call_wall, put_wall, zero, tuple(crossings),
                         dict(call), dict(put))
