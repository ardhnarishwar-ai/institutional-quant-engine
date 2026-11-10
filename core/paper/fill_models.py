"""Deterministic market-evidence fill models for paper simulation only."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import Optional, Tuple

from .contracts import PaperOrder


def _positive_decimal(value: object, name: str) -> Decimal:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be a positive finite decimal")
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value))
    except Exception as exc:
        raise ValueError(f"{name} must be a positive finite decimal") from exc
    if not result.is_finite() or result <= 0:
        raise ValueError(f"{name} must be a positive finite decimal")
    return result


def _timestamp(value: Optional[int], name: str, nullable: bool = False) -> None:
    if value is None and nullable:
        return
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{name} must be a non-negative integer timestamp")


@dataclass(frozen=True, slots=True)
class BookLevel:
    price: Decimal
    quantity: Decimal

    def __post_init__(self) -> None:
        object.__setattr__(self, "price", _positive_decimal(self.price, "price"))
        object.__setattr__(self, "quantity", _positive_decimal(self.quantity, "quantity"))


@dataclass(frozen=True, slots=True)
class MarketSnapshot:
    """One immutable, synchronized market-evidence boundary.

    Levels must be best-to-worst: bids descending and asks ascending.
    The exchange clock is nullable and is never inferred from another clock.
    """

    snapshot_id: str
    venue: str
    instrument_id: str
    bids: Tuple[BookLevel, ...]
    asks: Tuple[BookLevel, ...]
    event_ids: Tuple[str, ...]
    exchange_timestamp_ns: Optional[int]
    receive_timestamp_ns: int
    simulation_timestamp_ns: int
    feed_healthy: bool = True
    book_synchronized: bool = True

    def __post_init__(self) -> None:
        for name in ("snapshot_id", "venue", "instrument_id"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{name} must be a non-empty string")
        if not isinstance(self.bids, tuple) or not isinstance(self.asks, tuple):
            raise ValueError("bids and asks must be immutable tuples")
        if not self.bids or not self.asks:
            raise ValueError("both bid and ask sides are required")
        if any(not isinstance(level, BookLevel) for level in self.bids + self.asks):
            raise ValueError("book levels must be BookLevel instances")
        if tuple(sorted(self.bids, key=lambda x: x.price, reverse=True)) != self.bids:
            raise ValueError("bids must be sorted descending")
        if tuple(sorted(self.asks, key=lambda x: x.price)) != self.asks:
            raise ValueError("asks must be sorted ascending")
        if self.bids[0].price >= self.asks[0].price:
            raise ValueError("crossed or locked top of book is not valid fill evidence")
        if not isinstance(self.event_ids, tuple) or not self.event_ids:
            raise ValueError("event_ids must be a non-empty tuple")
        if any(not isinstance(item, str) or not item.strip() for item in self.event_ids):
            raise ValueError("event_ids must contain non-empty strings")
        if not isinstance(self.feed_healthy, bool) or not isinstance(self.book_synchronized, bool):
            raise ValueError("feed_healthy and book_synchronized must be bool")
        _timestamp(self.exchange_timestamp_ns, "exchange_timestamp_ns", nullable=True)
        _timestamp(self.receive_timestamp_ns, "receive_timestamp_ns")
        _timestamp(self.simulation_timestamp_ns, "simulation_timestamp_ns")


class FillReason(str, Enum):
    FILLED = "FILLED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    NOT_MARKETABLE = "NOT_MARKETABLE"
    NO_DISPLAYED_SIZE = "NO_DISPLAYED_SIZE"
    FEED_UNHEALTHY = "FEED_UNHEALTHY"
    BOOK_DESYNCHRONIZED = "BOOK_DESYNCHRONIZED"
    INSTRUMENT_MISMATCH = "INSTRUMENT_MISMATCH"
    VENUE_MISMATCH = "VENUE_MISMATCH"
    INVALID_DIRECTION = "INVALID_DIRECTION"


@dataclass(frozen=True, slots=True)
class FillProposal:
    price: Optional[Decimal]
    quantity: Decimal
    reason: FillReason
    model_version: str


class TopOfBookFillModel:
    """Conservative top-level marketable-limit model.

    No passive queue-position assumptions, touch fills, hidden liquidity,
    spread crossing beyond the limit, or fabricated trade events.
    """

    VERSION = "top-of-book-marketable-limit-v1"

    def propose(self, order: PaperOrder, snapshot: MarketSnapshot) -> FillProposal:
        if not snapshot.feed_healthy:
            return FillProposal(None, Decimal("0"), FillReason.FEED_UNHEALTHY, self.VERSION)
        if not snapshot.book_synchronized:
            return FillProposal(None, Decimal("0"), FillReason.BOOK_DESYNCHRONIZED, self.VERSION)
        if order.instrument_id != snapshot.instrument_id:
            return FillProposal(None, Decimal("0"), FillReason.INSTRUMENT_MISMATCH, self.VERSION)
        if order.venue != snapshot.venue:
            return FillProposal(None, Decimal("0"), FillReason.VENUE_MISMATCH, self.VERSION)

        remaining = order.quantity - order.filled_quantity
        if remaining <= 0:
            return FillProposal(None, Decimal("0"), FillReason.NO_DISPLAYED_SIZE, self.VERSION)

        direction = order.direction.upper()
        if direction == "LONG":
            level = snapshot.asks[0]
            marketable = level.price <= order.limit_price
        elif direction == "SHORT":
            level = snapshot.bids[0]
            marketable = level.price >= order.limit_price
        else:
            return FillProposal(None, Decimal("0"), FillReason.INVALID_DIRECTION, self.VERSION)

        if not marketable:
            return FillProposal(None, Decimal("0"), FillReason.NOT_MARKETABLE, self.VERSION)

        quantity = min(remaining, level.quantity)
        if quantity <= 0:
            return FillProposal(None, Decimal("0"), FillReason.NO_DISPLAYED_SIZE, self.VERSION)
        reason = FillReason.FILLED if quantity == remaining else FillReason.PARTIALLY_FILLED
        return FillProposal(level.price, quantity, reason, self.VERSION)
