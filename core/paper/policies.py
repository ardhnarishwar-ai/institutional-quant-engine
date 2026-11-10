"""Instrument-isolated paper policies; values are conservative placeholders."""

from dataclasses import dataclass
from decimal import Decimal
from typing import Dict


@dataclass(frozen=True, slots=True)
class PaperInstrumentPolicy:
    instrument_key: str
    venue: str
    tick_size: Decimal
    quantity_step: Decimal
    max_order_quantity: Decimal
    policy_version: str = "paper-policy-v1"

    def __post_init__(self) -> None:
        for name in ("instrument_key", "venue", "policy_version"):
            if not getattr(self, name):
                raise ValueError(f"{name} is required")
        for name in ("tick_size", "quantity_step", "max_order_quantity"):
            value = Decimal(str(getattr(self, name)))
            if not value.is_finite() or value <= 0:
                raise ValueError(f"{name} must be finite and positive")
            object.__setattr__(self, name, value)


def default_paper_policies() -> Dict[str, PaperInstrumentPolicy]:
    """Return identity/configuration shells, not exchange lot-size assertions.

    Production tick/quantity metadata must be loaded from verified instrument
    master data before enabling the corresponding simulation policy.
    """
    return {
        "NIFTY": PaperInstrumentPolicy("NIFTY", "NSE", Decimal("0.05"), Decimal("1"), Decimal("1")),
        "BANKNIFTY": PaperInstrumentPolicy("BANKNIFTY", "NSE", Decimal("0.05"), Decimal("1"), Decimal("1")),
        "FINNIFTY": PaperInstrumentPolicy("FINNIFTY", "NSE", Decimal("0.05"), Decimal("1"), Decimal("1")),
        "BTCUSDT": PaperInstrumentPolicy("BTCUSDT", "BINANCE", Decimal("0.01"), Decimal("0.001"), Decimal("0.001")),
    }
