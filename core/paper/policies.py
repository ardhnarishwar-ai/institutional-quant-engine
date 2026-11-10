"""Immutable, instrument-isolated paper-simulation policies.

No exchange specifications are guessed here. Tick size, quantity increments,
limits and participation caps must be supplied by a trusted instrument
master/configuration and versioned by the caller.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from types import MappingProxyType
from typing import Dict, Iterable, Mapping, Optional, Tuple


D = Decimal


class PolicyStatus(str, Enum):
    APPROVED = "APPROVED"
    HOLD = "HOLD"
    REJECTED = "REJECTED"


class PolicyReason(str, Enum):
    POLICY_NOT_FOUND = "POLICY_NOT_FOUND"
    POLICY_IDENTITY_MISMATCH = "POLICY_IDENTITY_MISMATCH"
    POLICY_VERSION_MISSING = "POLICY_VERSION_MISSING"
    EXECUTION_NOT_DISABLED = "EXECUTION_NOT_DISABLED"
    INVALID_SIDE = "INVALID_SIDE"
    INVALID_PRICE = "INVALID_PRICE"
    INVALID_QUANTITY = "INVALID_QUANTITY"
    PRICE_OFF_TICK = "PRICE_OFF_TICK"
    QUANTITY_OFF_STEP = "QUANTITY_OFF_STEP"
    BELOW_MINIMUM_QUANTITY = "BELOW_MINIMUM_QUANTITY"
    ORDER_QUANTITY_LIMIT = "ORDER_QUANTITY_LIMIT"
    POSITION_LIMIT = "POSITION_LIMIT"
    NOTIONAL_LIMIT = "NOTIONAL_LIMIT"
    MISSING_MARKET_METRIC = "MISSING_MARKET_METRIC"
    STALE_FEED = "STALE_FEED"
    DESYNCHRONIZED_BOOK = "DESYNCHRONIZED_BOOK"
    SPREAD_LIMIT = "SPREAD_LIMIT"
    PARTICIPATION_LIMIT = "PARTICIPATION_LIMIT"
    INVALID_CONTEXT = "INVALID_CONTEXT"


def _dec(value: object, name: str, *, positive: bool = False,
         non_negative: bool = False) -> Decimal:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be a finite Decimal-compatible value")
    try:
        result = value if isinstance(value, Decimal) else D(str(value))
    except Exception as exc:
        raise ValueError(f"{name} must be a finite Decimal-compatible value") from exc
    if not result.is_finite():
        raise ValueError(f"{name} must be finite")
    if positive and result <= 0:
        raise ValueError(f"{name} must be greater than zero")
    if non_negative and result < 0:
        raise ValueError(f"{name} must be non-negative")
    return result


@dataclass(frozen=True, slots=True)
class InstrumentPolicyKey:
    venue: str
    instrument_id: str

    def __post_init__(self) -> None:
        if not self.venue or not self.instrument_id:
            raise ValueError("venue and instrument_id must be non-empty")


@dataclass(frozen=True, slots=True)
class InstrumentSimulationPolicy:
    venue: str
    instrument_id: str
    policy_version: str
    tick_size: Decimal
    quantity_step: Decimal
    minimum_quantity: Decimal
    maximum_order_quantity: Decimal
    maximum_position_quantity: Decimal
    maximum_order_notional: Decimal
    maximum_spread_bps: Decimal
    maximum_participation_rate: Decimal
    maximum_leverage: Decimal = D("1")
    allow_short: bool = True
    execution_mode: str = "DISABLED"

    def __post_init__(self) -> None:
        if not self.venue or not self.instrument_id:
            raise ValueError("venue and instrument_id must be non-empty")
        if not isinstance(self.policy_version, str) or not self.policy_version.strip():
            raise ValueError("policy_version is required")
        for field in ("tick_size", "quantity_step", "minimum_quantity",
                      "maximum_order_quantity", "maximum_position_quantity",
                      "maximum_order_notional", "maximum_spread_bps",
                      "maximum_participation_rate", "maximum_leverage"):
            value = _dec(getattr(self, field), field,
                         positive=field not in ("maximum_spread_bps",),
                         non_negative=field == "maximum_spread_bps")
            object.__setattr__(self, field, value)
        if self.maximum_participation_rate > 1:
            raise ValueError("maximum_participation_rate must be in (0, 1]")
        if self.minimum_quantity > self.maximum_order_quantity:
            raise ValueError("minimum_quantity exceeds maximum_order_quantity")
        if self.maximum_order_quantity > self.maximum_position_quantity:
            raise ValueError("maximum_order_quantity exceeds maximum_position_quantity")
        if self.execution_mode != "DISABLED":
            raise ValueError("paper policy execution_mode must be DISABLED")
        if not isinstance(self.allow_short, bool):
            raise ValueError("allow_short must be bool")


@dataclass(frozen=True, slots=True)
class PolicyMarketContext:
    feed_fresh: Optional[bool]
    book_synchronized: Optional[bool]
    best_bid: Optional[Decimal]
    best_ask: Optional[Decimal]
    displayed_opposite_depth: Optional[Decimal]
    current_position_quantity: Decimal = D("0")


@dataclass(frozen=True, slots=True)
class PolicyDecision:
    status: PolicyStatus
    reasons: Tuple[PolicyReason, ...]
    venue: str
    instrument_id: str
    policy_version: Optional[str]
    allowed_quantity: Optional[Decimal] = None
    estimated_notional: Optional[Decimal] = None

    @property
    def approved(self) -> bool:
        return self.status is PolicyStatus.APPROVED


class InstrumentPolicyRegistry:
    """Immutable registry keyed by exact (venue, instrument_id) identity."""

    def __init__(self, policies: Iterable[InstrumentSimulationPolicy]) -> None:
        entries: Dict[InstrumentPolicyKey, InstrumentSimulationPolicy] = {}
        for policy in policies:
            key = InstrumentPolicyKey(policy.venue, policy.instrument_id)
            if key in entries:
                raise ValueError(f"duplicate policy for {key}")
            entries[key] = policy
        self._policies: Mapping[InstrumentPolicyKey, InstrumentSimulationPolicy] = (
            MappingProxyType(entries)
        )

    def get(self, venue: str, instrument_id: str) -> Optional[InstrumentSimulationPolicy]:
        return self._policies.get(InstrumentPolicyKey(venue, instrument_id))

    def keys(self) -> Tuple[InstrumentPolicyKey, ...]:
        return tuple(sorted(self._policies, key=lambda k: (k.venue, k.instrument_id)))


class InstrumentPolicyGate:
    """Pure deterministic validation; does not mutate orders, fills or ledger."""

    def __init__(self, registry: InstrumentPolicyRegistry) -> None:
        self.registry = registry

    @staticmethod
    def _result(status: PolicyStatus, reasons: list[PolicyReason], venue: str,
                instrument_id: str, policy: Optional[InstrumentSimulationPolicy],
                qty: Optional[Decimal] = None,
                notional: Optional[Decimal] = None) -> PolicyDecision:
        return PolicyDecision(status, tuple(reasons), venue, instrument_id,
                              policy.policy_version if policy else None, qty, notional)

    def evaluate(
        self,
        *,
        venue: str,
        instrument_id: str,
        side: str,
        price: object,
        quantity: object,
        context: PolicyMarketContext,
        execution_mode: str = "DISABLED",
    ) -> PolicyDecision:
        policy = self.registry.get(venue, instrument_id)
        if policy is None:
            return self._result(PolicyStatus.HOLD, [PolicyReason.POLICY_NOT_FOUND],
                                venue, instrument_id, None)
        if (policy.venue, policy.instrument_id) != (venue, instrument_id):
            return self._result(PolicyStatus.REJECTED,
                                [PolicyReason.POLICY_IDENTITY_MISMATCH],
                                venue, instrument_id, policy)
        if execution_mode != "DISABLED" or policy.execution_mode != "DISABLED":
            return self._result(PolicyStatus.REJECTED,
                                [PolicyReason.EXECUTION_NOT_DISABLED],
                                venue, instrument_id, policy)

        hard: list[PolicyReason] = []
        hold: list[PolicyReason] = []
        side = str(side).upper()
        if side not in ("BUY", "SELL"):
            hard.append(PolicyReason.INVALID_SIDE)
        if side == "SELL" and not policy.allow_short:
            try:
                current = _dec(context.current_position_quantity, "current_position_quantity")
                requested = _dec(quantity, "quantity", positive=True)
                if requested > current:
                    hard.append(PolicyReason.INVALID_SIDE)
            except ValueError:
                hard.append(PolicyReason.INVALID_CONTEXT)

        try:
            px = _dec(price, "price", positive=True)
        except ValueError:
            px = None
            hard.append(PolicyReason.INVALID_PRICE)
        try:
            qty = _dec(quantity, "quantity", positive=True)
        except ValueError:
            qty = None
            hard.append(PolicyReason.INVALID_QUANTITY)

        if hard:
            return self._result(PolicyStatus.REJECTED, hard, venue, instrument_id, policy)

        assert px is not None and qty is not None
        if px % policy.tick_size != 0:
            hard.append(PolicyReason.PRICE_OFF_TICK)
        if qty % policy.quantity_step != 0:
            hard.append(PolicyReason.QUANTITY_OFF_STEP)
        if qty < policy.minimum_quantity:
            hard.append(PolicyReason.BELOW_MINIMUM_QUANTITY)
        if qty > policy.maximum_order_quantity:
            hard.append(PolicyReason.ORDER_QUANTITY_LIMIT)
        projected = context.current_position_quantity + (qty if side == "BUY" else -qty)
        if abs(projected) > policy.maximum_position_quantity:
            hard.append(PolicyReason.POSITION_LIMIT)
        notional = px * qty
        if notional > policy.maximum_order_notional:
            hard.append(PolicyReason.NOTIONAL_LIMIT)

        if context.feed_fresh is None or context.book_synchronized is None:
            hold.append(PolicyReason.MISSING_MARKET_METRIC)
        elif not context.feed_fresh:
            hold.append(PolicyReason.STALE_FEED)
        elif not context.book_synchronized:
            hold.append(PolicyReason.DESYNCHRONIZED_BOOK)

        if (context.best_bid is None or context.best_ask is None
                or context.displayed_opposite_depth is None):
            hold.append(PolicyReason.MISSING_MARKET_METRIC)
        else:
            try:
                bid = _dec(context.best_bid, "best_bid", positive=True)
                ask = _dec(context.best_ask, "best_ask", positive=True)
                depth = _dec(context.displayed_opposite_depth, "displayed_opposite_depth",
                             non_negative=True)
                if ask < bid:
                    hard.append(PolicyReason.INVALID_CONTEXT)
                else:
                    mid = (ask + bid) / D("2")
                    spread_bps = (ask - bid) / mid * D("10000")
                    if spread_bps > policy.maximum_spread_bps:
                        hold.append(PolicyReason.SPREAD_LIMIT)
                    if depth <= 0:
                        hold.append(PolicyReason.MISSING_MARKET_METRIC)
                    elif qty > depth * policy.maximum_participation_rate:
                        hold.append(PolicyReason.PARTICIPATION_LIMIT)
            except ValueError:
                hard.append(PolicyReason.INVALID_CONTEXT)

        if hard:
            return self._result(PolicyStatus.REJECTED, hard, venue, instrument_id,
                                policy, qty, notional)
        if hold:
            return self._result(PolicyStatus.HOLD, hold, venue, instrument_id,
                                policy, qty, notional)
        return self._result(PolicyStatus.APPROVED, [], venue, instrument_id,
                            policy, qty, notional)
PaperInstrumentPolicy = InstrumentSimulationPolicy
default_paper_policies = None
