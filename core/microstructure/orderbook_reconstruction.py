"""L2 market-by-price depletion/replenishment reconstruction.

Depth reductions are not automatically executions: they may be cancellations.
A depth reduction is counted as executed depletion only when reconciled with an
aggressor trade at the same passive-side price within max_event_age_ms.
This is a market-by-price heuristic, not individual-order/iceberg identification.
"""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Dict, Optional, Tuple


@dataclass(frozen=True)
class OrderBookConfig:
    refill_window_ms: int = 1000
    min_depletion: float = 1.0
    confirmation_ratio: float = 0.50
    max_event_age_ms: int = 250

    def __post_init__(self):
        if self.refill_window_ms <= 0 or self.max_event_age_ms < 0:
            raise ValueError("time windows must be positive (max_event_age_ms may be zero)")
        if self.min_depletion <= 0:
            raise ValueError("min_depletion must be positive")
        if not 0 <= self.confirmation_ratio <= 1:
            raise ValueError("confirmation_ratio must be in [0,1]")


@dataclass
class LevelState:
    price: float
    displayed_qty: float
    unmatched_depth_reductions: Deque[Tuple[float, int]] = field(default_factory=deque)
    unmatched_trade_qty: Deque[Tuple[float, int]] = field(default_factory=deque)
    executed_depletion_qty: float = 0.0
    replenished_qty: float = 0.0
    first_execution_ms: Optional[int] = None
    last_execution_ms: Optional[int] = None
    best_price_preserved: bool = True


@dataclass(frozen=True)
class ReplenishmentResult:
    side: str
    price: float
    depleted_qty: float
    replenished_qty: float
    replenishment_ratio: float
    depletion_rate_per_second: float
    confirmed: bool
    best_price_preserved: bool
    timestamp_ms: int
    event_age_ms: int


@dataclass(frozen=True)
class OrderBookSnapshot:
    timestamp_ms: int
    best_bid: Optional[float]
    best_ask: Optional[float]
    bids: dict
    asks: dict


class OrderBookReconstruction:
    def __init__(self, config: OrderBookConfig = OrderBookConfig()):
        self.config = config
        self.bids: Dict[float, float] = {}
        self.asks: Dict[float, float] = {}
        self._levels: Dict[tuple, LevelState] = {}
        self._results: Deque[ReplenishmentResult] = deque(maxlen=1000)
        self._last_timestamp: Optional[int] = None

    def _validate_time(self, timestamp_ms: int):
        if not isinstance(timestamp_ms, int) or timestamp_ms < 0:
            raise ValueError("timestamp_ms must be a non-negative integer")
        if self._last_timestamp is not None and timestamp_ms < self._last_timestamp:
            raise ValueError("out-of-order event timestamp")
        self._last_timestamp = timestamp_ms

    def _book(self, side: str):
        side = side.upper()
        if side not in ("BID", "ASK"):
            raise ValueError("side must be BID or ASK")
        return self.bids if side == "BID" else self.asks

    def _state(self, side: str, price: float) -> LevelState:
        key = (side, float(price))
        if key not in self._levels:
            self._levels[key] = LevelState(float(price), self._book(side).get(float(price), 0.0))
        return self._levels[key]

    def _expire_unmatched(self, state: LevelState, now_ms: int):
        limit = self.config.max_event_age_ms
        while state.unmatched_depth_reductions and now_ms - state.unmatched_depth_reductions[0][1] > limit:
            state.unmatched_depth_reductions.popleft()
        while state.unmatched_trade_qty and now_ms - state.unmatched_trade_qty[0][1] > limit:
            state.unmatched_trade_qty.popleft()

    def _match(self, side: str, price: float, state: LevelState, now_ms: int):
        """FIFO-match trade prints with depth reductions within the age limit."""
        self._expire_unmatched(state, now_ms)
        while state.unmatched_depth_reductions and state.unmatched_trade_qty:
            depth_qty, depth_ts = state.unmatched_depth_reductions[0]
            trade_qty, trade_ts = state.unmatched_trade_qty[0]
            if abs(depth_ts - trade_ts) > self.config.max_event_age_ms:
                if depth_ts < trade_ts:
                    state.unmatched_depth_reductions.popleft()
                else:
                    state.unmatched_trade_qty.popleft()
                continue
            matched = min(depth_qty, trade_qty)
            state.executed_depletion_qty += matched
            execution_ts = max(depth_ts, trade_ts)
            if state.first_execution_ms is None:
                state.first_execution_ms = execution_ts
            state.last_execution_ms = execution_ts
            # The price must be the best passive price at execution time.
            book = self._book(side)
            best = (max(book) if side == "BID" and book else
                    min(book) if side == "ASK" and book else None)
            if best != float(price):
                state.best_price_preserved = False
            depth_qty -= matched
            trade_qty -= matched
            state.unmatched_depth_reductions.popleft()
            state.unmatched_trade_qty.popleft()
            if depth_qty > 1e-12:
                state.unmatched_depth_reductions.appendleft((depth_qty, depth_ts))
            if trade_qty > 1e-12:
                state.unmatched_trade_qty.appendleft((trade_qty, trade_ts))

    def _invalidate_lost_best_prices(self):
        for (side, price), state in self._levels.items():
            book = self._book(side)
            best = (max(book) if side == "BID" and book else
                    min(book) if side == "ASK" and book else None)
            # Strict: once the defended price ceases to be the best price,
            # this candidate remains invalid even if the price later returns.
            if state.executed_depletion_qty > 0 and best != price:
                state.best_price_preserved = False

    def apply_depth(self, timestamp_ms: int, side: str, price: float, quantity: float):
        self._validate_time(timestamp_ms)
        side = side.upper()
        book = self._book(side)
        if price <= 0 or quantity < 0:
            raise ValueError("price must be positive and quantity non-negative")
        price, quantity = float(price), float(quantity)
        old = book.get(price, 0.0)
        state = self._state(side, price)
        if quantity < old:
            state.unmatched_depth_reductions.append((old - quantity, timestamp_ms))
        elif quantity > old:
            increase = quantity - old
            if (state.executed_depletion_qty > state.replenished_qty and
                    state.last_execution_ms is not None and
                    timestamp_ms - state.last_execution_ms <= self.config.refill_window_ms and
                    state.best_price_preserved):
                eligible = state.executed_depletion_qty - state.replenished_qty
                state.replenished_qty += min(increase, eligible)
        if quantity == 0:
            book.pop(price, None)
        else:
            book[price] = quantity
        state.displayed_qty = quantity
        self._match(side, price, state, timestamp_ms)
        self._invalidate_lost_best_prices()
        return self.snapshot(timestamp_ms)

    def apply_trade(self, timestamp_ms: int, price: float, quantity: float, aggressor_side: str):
        self._validate_time(timestamp_ms)
        side = aggressor_side.upper()
        if side not in ("BUY", "SELL") or price <= 0 or quantity <= 0:
            raise ValueError("invalid trade")
        passive_side = "ASK" if side == "BUY" else "BID"
        price, quantity = float(price), float(quantity)
        state = self._state(passive_side, price)
        state.unmatched_trade_qty.append((quantity, timestamp_ms))
        self._match(passive_side, price, state, timestamp_ms)
        self._invalidate_lost_best_prices()
        if state.executed_depletion_qty < self.config.min_depletion:
            return None
        first_ms = state.first_execution_ms
        last_ms = state.last_execution_ms
        event_age = timestamp_ms - first_ms if first_ms is not None else 0
        elapsed = max(1, (last_ms or timestamp_ms) - first_ms) if first_ms is not None else 1
        ratio = state.replenished_qty / state.executed_depletion_qty
        fresh = event_age <= self.config.max_event_age_ms
        in_refill_window = (last_ms is not None and
                            timestamp_ms - last_ms <= self.config.refill_window_ms)
        confirmed = (fresh and in_refill_window and state.best_price_preserved and
                     ratio >= self.config.confirmation_ratio)
        result = ReplenishmentResult(
            passive_side, price, state.executed_depletion_qty,
            state.replenished_qty, ratio,
            state.executed_depletion_qty / (elapsed / 1000.0),
            confirmed, state.best_price_preserved, timestamp_ms, event_age,
        )
        self._results.append(result)
        return result

    def snapshot(self, timestamp_ms: Optional[int] = None):
        ts = self._last_timestamp if timestamp_ms is None else timestamp_ms
        return OrderBookSnapshot(
            ts if ts is not None else 0,
            max(self.bids) if self.bids else None,
            min(self.asks) if self.asks else None,
            dict(self.bids), dict(self.asks),
        )

    @property
    def results(self):
        return tuple(self._results)
