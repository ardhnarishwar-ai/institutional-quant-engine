"""Venue-agnostic session and rolling volume-at-price profiles.

Profiles consume canonical TRADE events only. No quote/LTP-to-trade inference
is performed. Explicit aggressor side contributes to buy/sell volume; unknown
side observations can be recorded through record_unclassified_trade() and
remain in total volume without being assigned to either side.

Prices are mapped to integer tick indices using nearest-tick rounding with
half-up semantics, avoiding floating-point dictionary-key drift.
"""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
from math import isfinite
from typing import Optional

from adapters.base.events import EventEnvelope, EventType, TradeEvent


class VolumeProfileError(ValueError):
    """Invalid profile input, identity mismatch, duplicate or time regression."""


@dataclass(frozen=True, slots=True)
class PriceLevel:
    price: float
    total_volume: float
    buy_volume: float
    sell_volume: float
    unknown_volume: float
    trade_count: int


@dataclass(frozen=True, slots=True)
class ProfileSnapshot:
    venue: str
    instrument_id: str
    session_id: str
    profile_kind: str
    event_count: int
    classified_trade_count: int
    unknown_trade_count: int
    total_volume: float
    buy_volume: float
    sell_volume: float
    unknown_volume: float
    poc: Optional[float]
    vah: Optional[float]
    val: Optional[float]
    value_area_percentage: float
    levels: tuple[PriceLevel, ...]


@dataclass(frozen=True, slots=True)
class _Print:
    tick: int
    price: float
    quantity: float
    side: Optional[str]
    event_id: str


class VolumeProfile:
    """One-instrument profile with session totals and last-N-print rolling view."""

    def __init__(
        self, *, tick_size: float, rolling_window: int = 1000,
        value_area_percentage: float = 70.0, session_id: str = "default",
    ):
        self.tick_size = self._positive_finite(tick_size, "tick_size")
        if not isinstance(rolling_window, int) or isinstance(rolling_window, bool) or rolling_window < 1:
            raise ValueError("rolling_window must be a positive integer")
        if not isinstance(value_area_percentage, (int, float)) or isinstance(value_area_percentage, bool) or not 0 < value_area_percentage <= 100:
            raise ValueError("value_area_percentage must be in (0, 100]")
        if not isinstance(session_id, str) or not session_id.strip():
            raise ValueError("session_id is required")
        self.rolling_window = rolling_window
        self.value_area_percentage = float(value_area_percentage)
        self.session_id = session_id
        self._venue: Optional[str] = None
        self._instrument_id: Optional[str] = None
        self._session: dict[int, list] = {}
        self._rolling: deque[_Print] = deque()
        self._seen_ids: set[str] = set()
        self._last_exchange_ns: Optional[int] = None
        self._last_receive_ns: Optional[int] = None
        self._event_count = self._classified_count = self._unknown_count = 0

    @staticmethod
    def _positive_finite(value, name):
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise TypeError(f"{name} must be numeric")
        value = float(value)
        if not isfinite(value) or value <= 0:
            raise ValueError(f"{name} must be finite and positive")
        return value

    def _tick(self, price: float) -> tuple[int, float]:
        p = Decimal(str(self._positive_finite(price, "price")))
        t = Decimal(str(self.tick_size))
        index = int((p / t).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
        normalized = float(t * index)
        if normalized <= 0:
            raise VolumeProfileError("price rounds to a non-positive tick")
        return index, normalized

    def _identity(self, venue: str, instrument_id: str):
        if self._venue is None:
            self._venue, self._instrument_id = venue, instrument_id
        elif (venue, instrument_id) != (self._venue, self._instrument_id):
            raise VolumeProfileError("profile is instrument-scoped; use VolumeProfileRegistry")

    def _time(self, exchange_ns, receive_ns):
        if exchange_ns is not None and (
            not isinstance(exchange_ns, int) or isinstance(exchange_ns, bool) or exchange_ns < 0
        ):
            raise ValueError("exchange_timestamp_ns must be None or a non-negative integer")
        if not isinstance(receive_ns, int) or isinstance(receive_ns, bool) or receive_ns < 0:
            raise ValueError("receive_timestamp_ns must be a non-negative integer")
        if self._last_exchange_ns is not None and exchange_ns is not None and exchange_ns < self._last_exchange_ns:
            raise VolumeProfileError("exchange timestamp regressed")
        if self._last_receive_ns is not None and receive_ns < self._last_receive_ns:
            raise VolumeProfileError("receive timestamp regressed")

    @staticmethod
    def _quantity(value):
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise TypeError("quantity must be numeric")
        value = float(value)
        if not isfinite(value) or value <= 0:
            raise ValueError("quantity must be finite and positive")
        return value

    def _add(self, *, venue, instrument_id, event_id, price, quantity, side,
             exchange_timestamp_ns, receive_timestamp_ns):
        if not isinstance(event_id, str) or not event_id.strip():
            raise ValueError("event_id is required")
        if event_id in self._seen_ids:
            raise VolumeProfileError(f"duplicate event_id: {event_id}")
        if side not in ("BUY", "SELL", None):
            raise ValueError("side must be BUY, SELL or None")
        self._identity(venue, instrument_id)
        self._time(exchange_timestamp_ns, receive_timestamp_ns)
        qty = self._quantity(quantity)
        tick, normalized_price = self._tick(price)

        # Session aggregation: [total, buy, sell, unknown, trade_count]
        level = self._session.setdefault(tick, [0.0, 0.0, 0.0, 0.0, 0])
        level[0] += qty
        if side == "BUY":
            level[1] += qty
            self._classified_count += 1
        elif side == "SELL":
            level[2] += qty
            self._classified_count += 1
        else:
            level[3] += qty
            self._unknown_count += 1
        level[4] += 1
        self._event_count += 1

        self._rolling.append(_Print(tick, normalized_price, qty, side, event_id))
        if len(self._rolling) > self.rolling_window:
            self._rolling.popleft()
        self._seen_ids.add(event_id)
        if exchange_timestamp_ns is not None:
            self._last_exchange_ns = exchange_timestamp_ns
        self._last_receive_ns = receive_timestamp_ns
        return self.session_snapshot()

    def update(self, event: EventEnvelope) -> ProfileSnapshot:
        if not isinstance(event, EventEnvelope):
            raise TypeError("event must be EventEnvelope")
        if event.event_type != EventType.TRADE or not isinstance(event.payload, TradeEvent):
            raise VolumeProfileError("volume profile accepts only canonical TRADE events")
        return self._add(
            venue=event.venue, instrument_id=event.instrument.canonical_id,
            event_id=event.event_id, price=event.payload.price,
            quantity=event.payload.quantity, side=event.payload.aggressor_side,
            exchange_timestamp_ns=event.exchange_timestamp_ns,
            receive_timestamp_ns=event.receive_timestamp_ns,
        )

    def record_unclassified_trade(
        self, *, venue: str, instrument_id: str, event_id: str, price: float,
        quantity: float, exchange_timestamp_ns: Optional[int],
        receive_timestamp_ns: int,
    ) -> ProfileSnapshot:
        """Record genuine trade volume when aggressor side is unavailable."""
        return self._add(
            venue=venue, instrument_id=instrument_id, event_id=event_id,
            price=price, quantity=quantity, side=None,
            exchange_timestamp_ns=exchange_timestamp_ns,
            receive_timestamp_ns=receive_timestamp_ns,
        )

    def _snapshot(self, aggregate: dict[int, list], *, kind: str,
                  event_count: int, classified_count: int, unknown_count: int) -> ProfileSnapshot:
        if not aggregate:
            return ProfileSnapshot(
                self._venue or "", self._instrument_id or "", self.session_id,
                kind, event_count, classified_count, unknown_count,
                0.0, 0.0, 0.0, 0.0, None, None, None,
                self.value_area_percentage, (),
            )
        ticks = sorted(aggregate)
        levels = tuple(
            PriceLevel(
                price=float(Decimal(str(self.tick_size)) * tick),
                total_volume=aggregate[tick][0],
                buy_volume=aggregate[tick][1],
                sell_volume=aggregate[tick][2],
                unknown_volume=aggregate[tick][3],
                trade_count=aggregate[tick][4],
            ) for tick in ticks
        )
        total = sum(x.total_volume for x in levels)
        buy = sum(x.buy_volume for x in levels)
        sell = sum(x.sell_volume for x in levels)
        unknown = sum(x.unknown_volume for x in levels)
        # POC tie-break: choose the lowest price tick deterministically.
        poc_tick = max(ticks, key=lambda k: (aggregate[k][0], -k))
        lo = hi = ticks.index(poc_tick)
        accumulated = aggregate[poc_tick][0]
        target = total * self.value_area_percentage / 100.0
        # Expand contiguously from POC. At equal adjacent volume, choose lower price.
        while accumulated < target and (lo > 0 or hi < len(ticks) - 1):
            lower_volume = aggregate[ticks[lo - 1]][0] if lo > 0 else -1.0
            upper_volume = aggregate[ticks[hi + 1]][0] if hi < len(ticks) - 1 else -1.0
            if lower_volume >= upper_volume:
                lo -= 1
                accumulated += lower_volume
            else:
                hi += 1
                accumulated += upper_volume
        return ProfileSnapshot(
            venue=self._venue or "", instrument_id=self._instrument_id or "",
            session_id=self.session_id, profile_kind=kind,
            event_count=event_count, classified_trade_count=classified_count,
            unknown_trade_count=unknown_count, total_volume=total,
            buy_volume=buy, sell_volume=sell, unknown_volume=unknown,
            poc=float(Decimal(str(self.tick_size)) * poc_tick),
            vah=float(Decimal(str(self.tick_size)) * ticks[hi]),
            val=float(Decimal(str(self.tick_size)) * ticks[lo]),
            value_area_percentage=self.value_area_percentage, levels=levels,
        )

    def session_snapshot(self) -> ProfileSnapshot:
        return self._snapshot(
            self._session, kind="SESSION", event_count=self._event_count,
            classified_count=self._classified_count, unknown_count=self._unknown_count,
        )

    def rolling_snapshot(self) -> ProfileSnapshot:
        aggregate: dict[int, list] = {}
        classified = unknown = 0
        for item in self._rolling:
            level = aggregate.setdefault(item.tick, [0.0, 0.0, 0.0, 0.0, 0])
            level[0] += item.quantity
            if item.side == "BUY":
                level[1] += item.quantity
                classified += 1
            elif item.side == "SELL":
                level[2] += item.quantity
                classified += 1
            else:
                level[3] += item.quantity
                unknown += 1
            level[4] += 1
        return self._snapshot(
            aggregate, kind="ROLLING", event_count=len(self._rolling),
            classified_count=classified, unknown_count=unknown,
        )

    def start_session(self, session_id: str):
        if not isinstance(session_id, str) or not session_id.strip():
            raise ValueError("session_id is required")
        if session_id == self.session_id:
            raise VolumeProfileError("new session_id must differ from current session")
        self.session_id = session_id
        self._session.clear()
        self._event_count = self._classified_count = self._unknown_count = 0
        self._rolling.clear()
        self._last_exchange_ns = self._last_receive_ns = None
        # Keep seen IDs across sessions to prevent replayed prints being counted twice.


class VolumeProfileRegistry:
    """Independent volume profiles keyed by venue and canonical instrument."""

    def __init__(self, *, tick_size_by_instrument: dict, rolling_window: int = 1000,
                 value_area_percentage: float = 70.0, session_id: str = "default"):
        self.tick_size_by_instrument = dict(tick_size_by_instrument)
        self.rolling_window = rolling_window
        self.value_area_percentage = value_area_percentage
        self.session_id = session_id
        self._profiles: dict[tuple[str, str], VolumeProfile] = {}

    def update(self, event: EventEnvelope) -> ProfileSnapshot:
        key = (event.venue, event.instrument.canonical_id)
        profile = self._profiles.get(key)
        if profile is None:
            if key not in self.tick_size_by_instrument:
                raise VolumeProfileError(f"tick size is not configured for {key[1]}")
            profile = VolumeProfile(
                tick_size=self.tick_size_by_instrument[key],
                rolling_window=self.rolling_window,
                value_area_percentage=self.value_area_percentage,
                session_id=self.session_id,
            )
            self._profiles[key] = profile
        return profile.update(event)

    def profile_for(self, venue: str, instrument_id: str) -> Optional[VolumeProfile]:
        return self._profiles.get((venue, instrument_id))

    def snapshots(self) -> tuple[ProfileSnapshot, ...]:
        return tuple(self._profiles[k].session_snapshot() for k in sorted(self._profiles))
