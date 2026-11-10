"""Immutable canonical event envelope and validated typed payload schemas."""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import math
from typing import TypeAlias

from adapters.base.instrument import InstrumentId


class EventType(str, Enum):
    TRADE = "TRADE"
    DEPTH_DELTA = "DEPTH_DELTA"
    DEPTH_DELTA_BATCH = "DEPTH_DELTA_BATCH"
    DEPTH_SNAPSHOT = "DEPTH_SNAPSHOT"
    OPTION_METRIC = "OPTION_METRIC"
    QUOTE = "QUOTE"


@dataclass(frozen=True, slots=True)
class QuoteEvent:
    # A quote update is not an individual trade print.
    last_price: float | None = None
    last_quantity: int | None = None
    cumulative_volume: int | None = None
    average_price: float | None = None
    open_price: float | None = None
    high_price: float | None = None
    low_price: float | None = None
    close_price: float | None = None
    total_buy_quantity: float | None = None
    total_sell_quantity: float | None = None
    open_interest: int | None = None

    def __post_init__(self):
        for name in ("last_price", "average_price", "open_price", "high_price", "low_price", "close_price"):
            value = getattr(self, name)
            if value is not None:
                _positive_finite(value, name)
        for name in ("last_quantity", "cumulative_volume", "open_interest"):
            value = getattr(self, name)
            if value is not None and (not isinstance(value, int) or isinstance(value, bool) or value < 0):
                raise ValueError(f"{name} must be a non-negative integer")
        for name in ("total_buy_quantity", "total_sell_quantity"):
            value = getattr(self, name)
            if value is not None:
                _finite(value, name)
                if value < 0:
                    raise ValueError(f"{name} cannot be negative")


@dataclass(frozen=True, slots=True)
class TradeEvent:
    price: float
    quantity: float
    aggressor_side: str

    def __post_init__(self):
        _positive_finite(self.price, "price")
        _positive_finite(self.quantity, "quantity")
        if self.aggressor_side not in ("BUY", "SELL"):
            raise ValueError("aggressor_side must be BUY or SELL")


@dataclass(frozen=True, slots=True)
class DepthDeltaEvent:
    side: str
    price: float
    quantity: float
    action: str = "SET"

    def __post_init__(self):
        if self.side not in ("BID", "ASK"):
            raise ValueError("side must be BID or ASK")
        _positive_finite(self.price, "price")
        _finite(self.quantity, "quantity")
        if self.quantity < 0:
            raise ValueError("quantity cannot be negative")
        if self.action not in ("SET", "DELETE"):
            raise ValueError("action must be SET or DELETE")
        if self.action == "DELETE" and self.quantity != 0:
            raise ValueError("DELETE action requires quantity=0")


# Compatibility export for atomic depth-batch consumers.
DepthDeltaChange = DepthDeltaEvent


@dataclass(frozen=True, slots=True)
class DepthSnapshotLevel:
    price: float
    quantity: float

    def __post_init__(self):
        _positive_finite(self.price, "price")
        _positive_finite(self.quantity, "quantity")


@dataclass(frozen=True, slots=True)
class DepthSnapshotEvent:
    bids: tuple[DepthSnapshotLevel, ...]
    asks: tuple[DepthSnapshotLevel, ...]
    snapshot_sequence: int | None

    def __post_init__(self):
        if self.snapshot_sequence is not None and (
            not isinstance(self.snapshot_sequence, int)
            or isinstance(self.snapshot_sequence, bool)
            or self.snapshot_sequence < 0
        ):
            raise ValueError("snapshot_sequence must be None or a non-negative integer")
        if not isinstance(self.bids, tuple) or not isinstance(self.asks, tuple):
            raise TypeError("bids and asks must be tuples")
        if any(not isinstance(x, DepthSnapshotLevel) for x in self.bids + self.asks):
            raise TypeError("snapshot levels must be DepthSnapshotLevel objects")
        if tuple(sorted(self.bids, key=lambda x: x.price, reverse=True)) != self.bids:
            raise ValueError("bids must be sorted best-to-worst")
        if tuple(sorted(self.asks, key=lambda x: x.price)) != self.asks:
            raise ValueError("asks must be sorted best-to-worst")
        if self.bids and self.asks and self.bids[0].price >= self.asks[0].price:
            raise ValueError("crossed snapshot book")


@dataclass(frozen=True, slots=True)
class OptionMetricEvent:
    open_interest: int
    implied_volatility: float
    underlying_price: float
    dealer_position_sign: int | None = None

    def __post_init__(self):
        if not isinstance(self.open_interest, int) or self.open_interest < 0:
            raise ValueError("open_interest must be a non-negative integer")
        _finite(self.implied_volatility, "implied_volatility")
        if self.implied_volatility < 0:
            raise ValueError("implied_volatility cannot be negative")
        _positive_finite(self.underlying_price, "underlying_price")
        if self.dealer_position_sign not in (None, -1, 1):
            raise ValueError("dealer_position_sign must be None, -1 or +1")


Payload: TypeAlias = QuoteEvent | TradeEvent | DepthDeltaEvent | DepthSnapshotEvent | OptionMetricEvent
_EXPECTED = {
    EventType.TRADE: TradeEvent,
    EventType.QUOTE: QuoteEvent,
    EventType.DEPTH_DELTA: DepthDeltaEvent,
    EventType.DEPTH_SNAPSHOT: DepthSnapshotEvent,
    EventType.OPTION_METRIC: OptionMetricEvent,
}


@dataclass(frozen=True, slots=True)
class EventEnvelope:
    venue: str
    instrument: InstrumentId
    event_type: EventType
    exchange_timestamp_ns: int | None
    receive_timestamp_ns: int
    sequence_number: int | None
    sequence_domain: str | None
    payload: Payload
    event_id: str
    schema_version: int = 1
    causal_parent_id: str | None = None

    def __post_init__(self):
        if not isinstance(self.instrument, InstrumentId):
            raise TypeError("instrument must be InstrumentId")
        venue = self.venue.strip().upper()
        if not venue:
            raise ValueError("venue is required")
        object.__setattr__(self, "venue", venue)
        if venue != self.instrument.venue:
            raise ValueError("envelope venue must match instrument venue")
        if not isinstance(self.event_type, EventType):
            raise TypeError("event_type must be EventType")
        if self.exchange_timestamp_ns is not None:
            _nonnegative_int(self.exchange_timestamp_ns, "exchange_timestamp_ns")
        _nonnegative_int(self.receive_timestamp_ns, "receive_timestamp_ns")
        if self.sequence_number is None:
            if self.sequence_domain is not None:
                raise ValueError("sequence_domain requires sequence_number")
        else:
            _nonnegative_int(self.sequence_number, "sequence_number")
            if not isinstance(self.sequence_domain, str) or not self.sequence_domain.strip():
                raise ValueError("sequence_number requires a non-empty sequence_domain")
        if self.event_type == EventType.DEPTH_DELTA_BATCH:
            from adapters.base.depth_batch import DepthDeltaBatchEvent
            expected = DepthDeltaBatchEvent
        else:
            expected = _EXPECTED[self.event_type]
        if not isinstance(self.payload, expected):
            raise TypeError(f"{self.event_type.value} requires {expected.__name__}")
        if not isinstance(self.event_id, str) or not self.event_id.strip():
            raise ValueError("event_id is required")
        if not isinstance(self.schema_version, int) or self.schema_version < 1:
            raise ValueError("schema_version must be a positive integer")
        if self.causal_parent_id is not None and not self.causal_parent_id.strip():
            raise ValueError("causal_parent_id cannot be blank")


def _finite(value, name):
    if not isinstance(value, (int, float)) or isinstance(value, bool) or not math.isfinite(value):
        raise ValueError(f"{name} must be finite numeric")


def _positive_finite(value, name):
    _finite(value, name)
    if value <= 0:
        raise ValueError(f"{name} must be positive")


def _nonnegative_int(value, name):
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ValueError(f"{name} must be a non-negative integer")
