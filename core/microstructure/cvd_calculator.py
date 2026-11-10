"""Event-driven, venue-agnostic cumulative volume delta.

Only canonical TRADE events with an explicit BUY/SELL aggressor side affect
directional CVD. QUOTE/LTP updates are never converted into trades. Unclassified
trade observations can be recorded separately; their volume remains UNKNOWN
and never enters buy volume, sell volume, or CVD.

Exchange and receive timestamps are retained independently. A missing exchange
timestamp remains None; it is never replaced with receive time.
"""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Iterable, Optional

from adapters.base.events import EventEnvelope, EventType, TradeEvent


class CVDValidationError(ValueError):
    """Invalid trade-flow input or event ordering."""


@dataclass(frozen=True, slots=True)
class CVDResult:
    instrument_id: str
    venue: str
    session_id: str
    event_id: str
    exchange_timestamp_ns: Optional[int]
    receive_timestamp_ns: int
    buy_volume: float
    sell_volume: float
    unknown_volume: float
    total_observed_volume: float
    delta: float
    cumulative_delta: float
    rolling_delta: float
    classified_volume: float
    classified_delta_ratio: Optional[float]
    classified_trade_count: int
    unknown_trade_count: int


@dataclass(frozen=True, slots=True)
class CVDReplayResult:
    event_count: int
    classified_trade_count: int
    ignored_non_trade_count: int
    unknown_trade_count: int
    final_results: tuple[CVDResult, ...]


class CVDCalculator:
    """Stateful CVD calculator for one instrument and one active session."""

    def __init__(self, *, rolling_window: int = 1000, session_id: str = "default"):
        if not isinstance(rolling_window, int) or isinstance(rolling_window, bool) or rolling_window < 1:
            raise ValueError("rolling_window must be a positive integer")
        if not isinstance(session_id, str) or not session_id.strip():
            raise ValueError("session_id is required")
        self.rolling_window = rolling_window
        self.session_id = session_id
        self._venue: Optional[str] = None
        self._instrument_id: Optional[str] = None
        self._buy = self._sell = self._unknown = 0.0
        self._rolling = deque()
        self._rolling_sum = 0.0
        self._seen_ids: set[str] = set()
        self._last_exchange_ns: Optional[int] = None
        self._last_receive_ns: Optional[int] = None
        self._classified_count = self._unknown_count = 0
        self._last_result: Optional[CVDResult] = None

    @staticmethod
    def _positive_quantity(value: float) -> float:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise TypeError("quantity must be numeric")
        value = float(value)
        if not (value > 0 and value < float("inf")):
            raise ValueError("quantity must be finite and positive")
        return value

    def _check_identity(self, event: EventEnvelope) -> tuple[str, str]:
        venue = event.venue
        instrument_id = event.instrument.canonical_id
        if self._venue is None:
            self._venue, self._instrument_id = venue, instrument_id
        elif (venue, instrument_id) != (self._venue, self._instrument_id):
            raise CVDValidationError("calculator is instrument-scoped; use CVDRegistry for multiple instruments")
        return venue, instrument_id

    def _check_timestamps(self, exchange_ns, receive_ns):
        if self._last_exchange_ns is not None and exchange_ns is not None and exchange_ns < self._last_exchange_ns:
            raise CVDValidationError("exchange timestamp regressed")
        if self._last_receive_ns is not None and receive_ns < self._last_receive_ns:
            raise CVDValidationError("receive timestamp regressed")

    def _append_delta(self, delta: float):
        self._rolling.append(delta)
        self._rolling_sum += delta
        if len(self._rolling) > self.rolling_window:
            self._rolling_sum -= self._rolling.popleft()

    def _result(self, event_id, exchange_ns, receive_ns, delta):
        classified = self._buy + self._sell
        total = classified + self._unknown
        return CVDResult(
            instrument_id=self._instrument_id or "",
            venue=self._venue or "",
            session_id=self.session_id,
            event_id=event_id,
            exchange_timestamp_ns=exchange_ns,
            receive_timestamp_ns=receive_ns,
            buy_volume=self._buy,
            sell_volume=self._sell,
            unknown_volume=self._unknown,
            total_observed_volume=total,
            delta=delta,
            cumulative_delta=self._buy - self._sell,
            rolling_delta=self._rolling_sum,
            classified_volume=classified,
            classified_delta_ratio=((self._buy - self._sell) / classified if classified else None),
            classified_trade_count=self._classified_count,
            unknown_trade_count=self._unknown_count,
        )

    def update(self, event: EventEnvelope) -> CVDResult:
        """Apply one classified canonical TRADE event; quotes are rejected."""
        if not isinstance(event, EventEnvelope):
            raise TypeError("event must be EventEnvelope")
        if event.event_type != EventType.TRADE or not isinstance(event.payload, TradeEvent):
            raise CVDValidationError("CVD accepts only canonical TRADE events with TradeEvent payloads")
        if event.event_id in self._seen_ids:
            raise CVDValidationError(f"duplicate event_id: {event.event_id}")
        venue, instrument_id = self._check_identity(event)
        self._check_timestamps(event.exchange_timestamp_ns, event.receive_timestamp_ns)
        quantity = self._positive_quantity(event.payload.quantity)
        side = event.payload.aggressor_side
        if side == "BUY":
            self._buy += quantity
            delta = quantity
        elif side == "SELL":
            self._sell += quantity
            delta = -quantity
        else:  # Defensive: TradeEvent schema currently only permits BUY/SELL.
            raise CVDValidationError("aggressor side must be explicitly BUY or SELL")
        self._classified_count += 1
        self._append_delta(delta)
        self._seen_ids.add(event.event_id)
        self._last_exchange_ns = event.exchange_timestamp_ns if event.exchange_timestamp_ns is not None else self._last_exchange_ns
        self._last_receive_ns = event.receive_timestamp_ns
        self._last_result = self._result(event.event_id, event.exchange_timestamp_ns, event.receive_timestamp_ns, delta)
        return self._last_result

    def record_unclassified_trade(self, *, event_id: str, exchange_timestamp_ns: Optional[int],
                                  receive_timestamp_ns: int, quantity: float) -> CVDResult:
        """Record volume with unknown aggressor side without guessing direction."""
        if not isinstance(event_id, str) or not event_id.strip():
            raise ValueError("event_id is required")
        if event_id in self._seen_ids:
            raise CVDValidationError(f"duplicate event_id: {event_id}")
        quantity = self._positive_quantity(quantity)
        if exchange_timestamp_ns is not None and (
            not isinstance(exchange_timestamp_ns, int) or isinstance(exchange_timestamp_ns, bool) or exchange_timestamp_ns < 0
        ):
            raise ValueError("exchange_timestamp_ns must be None or a non-negative integer")
        if not isinstance(receive_timestamp_ns, int) or isinstance(receive_timestamp_ns, bool) or receive_timestamp_ns < 0:
            raise ValueError("receive_timestamp_ns must be a non-negative integer")
        self._check_timestamps(exchange_timestamp_ns, receive_timestamp_ns)
        self._unknown += quantity
        self._unknown_count += 1
        self._seen_ids.add(event_id)
        self._last_exchange_ns = exchange_timestamp_ns if exchange_timestamp_ns is not None else self._last_exchange_ns
        self._last_receive_ns = receive_timestamp_ns
        self._last_result = self._result(event_id, exchange_timestamp_ns, receive_timestamp_ns, 0.0)
        return self._last_result

    def start_session(self, session_id: str) -> None:
        """Reset session totals while retaining instrument identity and dedupe history."""
        if not isinstance(session_id, str) or not session_id.strip():
            raise ValueError("session_id is required")
        if session_id == self.session_id:
            raise CVDValidationError("new session_id must differ from current session")
        self.session_id = session_id
        self._buy = self._sell = self._unknown = 0.0
        self._rolling.clear()
        self._rolling_sum = 0.0
        self._last_exchange_ns = self._last_receive_ns = None
        self._classified_count = self._unknown_count = 0
        self._last_result = None

    @property
    def snapshot(self) -> Optional[CVDResult]:
        return self._last_result


class CVDRegistry:
    """Maintains independent calculators keyed by venue and canonical instrument."""

    def __init__(self, *, rolling_window: int = 1000, session_id: str = "default"):
        self.rolling_window = rolling_window
        self.session_id = session_id
        self._calculators: dict[tuple[str, str], CVDCalculator] = {}

    def calculator_for(self, event: EventEnvelope) -> CVDCalculator:
        key = (event.venue, event.instrument.canonical_id)
        if key not in self._calculators:
            self._calculators[key] = CVDCalculator(
                rolling_window=self.rolling_window, session_id=self.session_id
            )
        return self._calculators[key]

    def update(self, event: EventEnvelope) -> CVDResult:
        return self.calculator_for(event).update(event)

    def snapshots(self) -> tuple[CVDResult, ...]:
        return tuple(self._calculators[key].snapshot for key in sorted(self._calculators)
                     if self._calculators[key].snapshot is not None)


def verify_cvd_replay(events: Iterable[EventEnvelope], *, rolling_window: int = 1000,
                      session_id: str = "replay") -> CVDReplayResult:
    """Deterministically replay canonical events; non-trade events are ignored."""
    registry = CVDRegistry(rolling_window=rolling_window, session_id=session_id)
    event_count = classified = ignored = 0
    for event in events:
        event_count += 1
        if event.event_type != EventType.TRADE:
            ignored += 1
            continue
        registry.update(event)
        classified += 1
    return CVDReplayResult(event_count, classified, ignored, 0, registry.snapshots())
