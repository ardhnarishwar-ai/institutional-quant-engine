"""Transparent, configurable transaction-cost calculations for paper fills."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
from typing import Dict


D = Decimal
BPS = D("10000")


def _decimal(value: object, name: str, *, non_negative: bool = True) -> Decimal:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be a finite Decimal-compatible value")
    try:
        result = value if isinstance(value, Decimal) else D(str(value))
    except Exception as exc:
        raise ValueError(f"{name} must be a finite Decimal-compatible value") from exc
    if not result.is_finite() or (non_negative and result < 0):
        raise ValueError(f"{name} must be finite and non-negative")
    return result


@dataclass(frozen=True, slots=True)
class FeeSchedule:
    """All rates are basis points; no jurisdiction-specific rate is assumed.

    fee_bps is the broker/execution fee. Other components are explicit
    configurable pass-through rates. GST is applied to fee + exchange +
    regulatory charges, not to notional or transaction tax.
    """

    fee_bps: Decimal = D("0")
    transaction_tax_bps: Decimal = D("0")
    exchange_bps: Decimal = D("0")
    regulatory_bps: Decimal = D("0")
    gst_bps: Decimal = D("0")
    currency_quantum: Decimal = D("0.01")

    def __post_init__(self) -> None:
        for name in ("fee_bps", "transaction_tax_bps", "exchange_bps",
                     "regulatory_bps", "gst_bps"):
            object.__setattr__(self, name, _decimal(getattr(self, name), name))
        quantum = _decimal(self.currency_quantum, "currency_quantum")
        if quantum <= 0:
            raise ValueError("currency_quantum must be greater than zero")
        object.__setattr__(self, "currency_quantum", quantum)


@dataclass(frozen=True, slots=True)
class CostBreakdown:
    notional: Decimal
    fee: Decimal
    transaction_tax: Decimal
    exchange_charge: Decimal
    regulatory_charge: Decimal
    gst: Decimal
    total: Decimal
    currency: str
    schedule_version: str


class TransactionCostModel:
    VERSION = "bps-cost-model-v1"

    def __init__(self, schedule: FeeSchedule, currency: str = "INR") -> None:
        if not isinstance(currency, str) or not currency.strip():
            raise ValueError("currency must be a non-empty code")
        self.schedule = schedule
        self.currency = currency.upper()

    def _round(self, amount: Decimal) -> Decimal:
        q = self.schedule.currency_quantum
        return (amount / q).quantize(D("1"), rounding=ROUND_HALF_UP) * q

    def calculate(self, price: Decimal, quantity: Decimal) -> CostBreakdown:
        price = _decimal(price, "price")
        quantity = _decimal(quantity, "quantity")
        if price <= 0 or quantity <= 0:
            raise ValueError("price and quantity must be greater than zero")
        notional = price * quantity
        s = self.schedule
        fee = self._round(notional * s.fee_bps / BPS)
        tax = self._round(notional * s.transaction_tax_bps / BPS)
        exchange = self._round(notional * s.exchange_bps / BPS)
        regulatory = self._round(notional * s.regulatory_bps / BPS)
        gst = self._round((fee + exchange + regulatory) * s.gst_bps / BPS)
        total = fee + tax + exchange + regulatory + gst
        return CostBreakdown(
            notional=notional, fee=fee, transaction_tax=tax,
            exchange_charge=exchange, regulatory_charge=regulatory,
            gst=gst, total=total, currency=self.currency,
            schedule_version=self.VERSION,
        )
