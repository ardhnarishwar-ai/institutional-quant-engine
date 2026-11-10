"""Immutable contracts for paper simulation; deliberately no broker API."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Optional, Tuple


def _decimal(value: object, field_name: str, *, positive: bool = False) -> Decimal:
    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be numeric, not bool")
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be a finite decimal") from exc
    if not result.is_finite():
        raise ValueError(f"{field_name} must be finite")
    if positive and result <= 0:
        raise ValueError(f"{field_name} must be > 0")
    return result


def _timestamp(value: Optional[int], field_name: str, *, nullable: bool = False) -> None:
    if value is None and nullable:
        return
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field_name} must be a non-negative integer nanosecond timestamp")


class PaperOrderType(str, Enum):
    LIMIT = "LIMIT"
    MARKETABLE_LIMIT = "MARKETABLE_LIMIT"


class PaperOrderStatus(str, Enum):
    CREATED = "CREATED"
    ACTIVE = "ACTIVE"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    EXPIRED = "EXPIRED"
    REJECTED = "REJECTED"
    INVALIDATED = "INVALIDATED"


class ExecutionSource(str, Enum):
    PAPER_SIMULATOR = "PAPER_SIMULATOR"


@dataclass(frozen=True, slots=True)
class PaperOrder:
    order_id: str
    decision_id: str
    venue: str
    instrument_id: str
    direction: str
    order_type: PaperOrderType
    limit_price: Decimal
    quantity: Decimal
    created_exchange_timestamp_ns: Optional[int]
    created_receive_timestamp_ns: int
    created_simulation_timestamp_ns: int
    source_event_ids: Tuple[str, ...]
    status: PaperOrderStatus = PaperOrderStatus.CREATED
    filled_quantity: Decimal = Decimal("0")
    policy_version: str = "paper-v1"

    def __post_init__(self) -> None:
        for name in ("order_id", "decision_id", "venue", "instrument_id", "direction", "policy_version"):
            if not isinstance(getattr(self, name), str) or not getattr(self, name).strip():
                raise ValueError(f"{name} must be a non-empty string")
        if not isinstance(self.order_type, PaperOrderType):
            raise ValueError("order_type must be PaperOrderType")
        if not isinstance(self.status, PaperOrderStatus):
            raise ValueError("status must be PaperOrderStatus")
        object.__setattr__(self, "limit_price", _decimal(self.limit_price, "limit_price", positive=True))
        object.__setattr__(self, "quantity", _decimal(self.quantity, "quantity", positive=True))
        filled = _decimal(self.filled_quantity, "filled_quantity")
        if filled < 0 or filled > self.quantity:
            raise ValueError("filled_quantity must be between zero and quantity")
        object.__setattr__(self, "filled_quantity", filled)
        _timestamp(self.created_exchange_timestamp_ns, "created_exchange_timestamp_ns", nullable=True)
        _timestamp(self.created_receive_timestamp_ns, "created_receive_timestamp_ns")
        _timestamp(self.created_simulation_timestamp_ns, "created_simulation_timestamp_ns")
        if not isinstance(self.source_event_ids, tuple) or not self.source_event_ids:
            raise ValueError("source_event_ids must be a non-empty tuple")
        if any(not isinstance(item, str) or not item.strip() for item in self.source_event_ids):
            raise ValueError("source_event_ids must contain non-empty strings")


@dataclass(frozen=True, slots=True)
class PaperFill:
    fill_id: str
    order_id: str
    venue: str
    instrument_id: str
    side: str
    price: Decimal
    quantity: Decimal
    execution_source: ExecutionSource
    market_evidence_event_ids: Tuple[str, ...]
    fill_model_version: str
    exchange_timestamp_ns: Optional[int]
    receive_timestamp_ns: int
    simulation_timestamp_ns: int

    def __post_init__(self) -> None:
        for name in ("fill_id", "order_id", "venue", "instrument_id", "fill_model_version"):
            if not isinstance(getattr(self, name), str) or not getattr(self, name).strip():
                raise ValueError(f"{name} must be a non-empty string")
        if self.side not in ("BUY", "SELL"):
            raise ValueError("side must be BUY or SELL")
        if self.execution_source is not ExecutionSource.PAPER_SIMULATOR:
            raise ValueError("execution_source must be PAPER_SIMULATOR")
        object.__setattr__(self, "price", _decimal(self.price, "price", positive=True))
        object.__setattr__(self, "quantity", _decimal(self.quantity, "quantity", positive=True))
        if not isinstance(self.market_evidence_event_ids, tuple) or not self.market_evidence_event_ids:
            raise ValueError("market_evidence_event_ids must be a non-empty tuple")
        if any(not isinstance(item, str) or not item.strip() for item in self.market_evidence_event_ids):
            raise ValueError("market_evidence_event_ids must contain non-empty strings")
        _timestamp(self.exchange_timestamp_ns, "exchange_timestamp_ns", nullable=True)
        _timestamp(self.receive_timestamp_ns, "receive_timestamp_ns")
        _timestamp(self.simulation_timestamp_ns, "simulation_timestamp_ns")


@dataclass(frozen=True, slots=True)
class PaperPosition:
    venue: str
    instrument_id: str
    quantity: Decimal
    average_entry_price: Optional[Decimal]
    realized_pnl: Decimal = Decimal("0")
    mark_price: Optional[Decimal] = None
    last_exchange_timestamp_ns: Optional[int] = None
    last_receive_timestamp_ns: Optional[int] = None

    def __post_init__(self) -> None:
        if not self.venue or not self.instrument_id:
            raise ValueError("venue and instrument_id are required")
        object.__setattr__(self, "quantity", _decimal(self.quantity, "quantity"))
        object.__setattr__(self, "realized_pnl", _decimal(self.realized_pnl, "realized_pnl"))
        if self.average_entry_price is not None:
            object.__setattr__(self, "average_entry_price", _decimal(self.average_entry_price, "average_entry_price", positive=True))
        if self.mark_price is not None:
            object.__setattr__(self, "mark_price", _decimal(self.mark_price, "mark_price", positive=True))
        _timestamp(self.last_exchange_timestamp_ns, "last_exchange_timestamp_ns", nullable=True)
        _timestamp(self.last_receive_timestamp_ns, "last_receive_timestamp_ns", nullable=True)


@dataclass(frozen=True, slots=True)
class PaperAccount:
    account_id: str
    currency: str
    starting_cash: Decimal
    cash: Decimal
    reserved_margin: Decimal = Decimal("0")
    realized_pnl: Decimal = Decimal("0")
    account_version: str = "paper-account-v1"

    def __post_init__(self) -> None:
        if not self.account_id or not self.currency or not self.account_version:
            raise ValueError("account_id, currency and account_version are required")
        for name in ("starting_cash", "cash", "reserved_margin", "realized_pnl"):
            value = _decimal(getattr(self, name), name)
            if name in ("starting_cash", "reserved_margin") and value < 0:
                raise ValueError(f"{name} cannot be negative")
            object.__setattr__(self, name, value)
