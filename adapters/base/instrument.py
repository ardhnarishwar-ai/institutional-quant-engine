"""Canonical, venue-qualified instrument identifiers.

Identifiers are deliberately descriptive and stable; they are not broker
scrip tokens. Venue-specific tokens belong in adapter metadata mappings.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import re


class InstrumentKind(str, Enum):
    NSE_SPOT = "NSE_SPOT"
    NSE_FUTURE = "NSE_FUTURE"
    NSE_OPTION = "NSE_OPTION"
    CRYPTO_PERPETUAL = "CRYPTO_PERPETUAL"


_SYMBOL = re.compile(r"^[A-Z0-9][A-Z0-9._-]{0,31}$")
_CURRENCY = re.compile(r"^[A-Z0-9]{2,12}$")


@dataclass(frozen=True, slots=True)
class InstrumentId:
    venue: str
    kind: InstrumentKind
    symbol: str
    quote_currency: str = "INR"
    expiry: str | None = None
    strike: float | None = None
    option_type: str | None = None

    def __post_init__(self):
        venue = self.venue.strip().upper()
        symbol = self.symbol.strip().upper()
        quote = self.quote_currency.strip().upper()
        if not venue or not re.fullmatch(r"[A-Z0-9_-]{2,24}", venue):
            raise ValueError("venue must be a non-empty uppercase identifier")
        if not _SYMBOL.fullmatch(symbol):
            raise ValueError("invalid instrument symbol")
        if not _CURRENCY.fullmatch(quote):
            raise ValueError("invalid quote_currency")
        object.__setattr__(self, "venue", venue)
        object.__setattr__(self, "symbol", symbol)
        object.__setattr__(self, "quote_currency", quote)

        if self.kind == InstrumentKind.NSE_SPOT:
            if self.expiry is not None or self.strike is not None or self.option_type is not None:
                raise ValueError("NSE spot must not carry derivative fields")
            if quote != "INR":
                raise ValueError("NSE spot quote currency must be INR")
        elif self.kind == InstrumentKind.NSE_FUTURE:
            if not self.expiry or self.strike is not None or self.option_type is not None:
                raise ValueError("NSE future requires expiry and forbids strike/option_type")
            self._validate_expiry(self.expiry)
            if quote != "INR":
                raise ValueError("NSE future quote currency must be INR")
        elif self.kind == InstrumentKind.NSE_OPTION:
            if not self.expiry or self.strike is None or self.strike <= 0:
                raise ValueError("NSE option requires expiry and positive strike")
            if self.option_type not in ("CE", "PE"):
                raise ValueError("NSE option option_type must be CE or PE")
            self._validate_expiry(self.expiry)
            if quote != "INR":
                raise ValueError("NSE option quote currency must be INR")
        elif self.kind == InstrumentKind.CRYPTO_PERPETUAL:
            if self.expiry is not None or self.strike is not None or self.option_type is not None:
                raise ValueError("crypto perpetual must not carry expiry/option fields")
            if not venue or quote == "INR":
                raise ValueError("crypto perpetual requires a crypto quote currency")
        else:
            raise ValueError(f"unsupported instrument kind: {self.kind!r}")

    @staticmethod
    def _validate_expiry(value: str):
        # Canonical expiry representation is YYYY-MM-DD.
        from datetime import date
        try:
            parsed = date.fromisoformat(value)
        except (TypeError, ValueError) as exc:
            raise ValueError("expiry must be YYYY-MM-DD") from exc
        if parsed.isoformat() != value:
            raise ValueError("expiry must be canonical YYYY-MM-DD")

    @property
    def canonical_id(self) -> str:
        base = f"{self.venue}:{self.kind.value}:{self.symbol}:{self.quote_currency}"
        if self.kind == InstrumentKind.NSE_FUTURE:
            return f"{base}:{self.expiry}"
        if self.kind == InstrumentKind.NSE_OPTION:
            return f"{base}:{self.expiry}:{self.strike:g}:{self.option_type}"
        return base
