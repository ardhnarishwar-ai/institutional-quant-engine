"""L2 market-by-price reconstruction and heuristic displayed-liquidity refill.

This module does not identify individual orders or prove iceberg activity.
It reconciles trade prints with displayed depth reductions at price levels.
Feed sequence validation, gap recovery and exchange-specific normalization
belong in the venue adapter.
"""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Deque, Dict, Optional


@dataclass(frozen=True)
class OrderBookConfig:
    refill_window_ms: int = 1000
    min_depletion: float = 1.0
    confirmation_ratio: float = 0.50
    max_event_age_ms: int = 5000

    def __post_init__(self):
        if self.refill_window_ms <= 0 or self.min_depletion <= 0:
            raise ValueError("time window and min_depletion must be positive")
        if not 0 <= self.confirmation_ratio <= 1:
            raise ValueError("confirmation_ratio must be in [0,1]")


@dataclass
class LevelState:
    price: float
    initial_qty: float
    displayed_qty: float
    depleted_qty: float = 0.0
    replenished_qty: float = 0.0
    first_depletion_ms: Optional[int] = None
    last_update_ms: Optional[int] = None
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
        self._last_timestamp = None

    def _side_book(self, side):
        side = side.upper()
        if side not in ("BID", "ASK"):
            raise ValueError("side must be BID or ASK")
        return self.bids if side == "BID" else self.asks

    def apply_depth(self, timestamp_ms: int, side: str, price: float, quantity: float):
        if timestamp_ms < 0 or price <= 0 or quantity < 0:
            raise ValueError("invalid depth update")
        if self._last_timestamp is not None and timestamp_ms < self._last_timestamp:
            raise ValueError("out-of-order timestamp")
        self._last_timestamp = timestamp_ms
        book = self._side_book(side)
        old = book.get(float(price), 0.0)
        new = float(quantity)
        key = (side.upper(), float(price))
        state = self._levels.get(key)
        if state is None and old > 0:
            state = LevelState(float(price), old, old, last_update_ms=timestamp_ms)
            self._levels[key] = state
        if state is not None:
            if new < old:
                reduction = old - new
                state.depleted_qty += reduction
                if state.first_depletion_ms is None:
                    state.first_depletion_ms = timestamp_ms
            elif new > old and state.depleted_qty > state.replenished_qty:
                if state.first_depletion_ms is not None and timestamp_ms - state.first_depletion_ms <= self.config.refill_window_ms:
                    state.replenished_qty += min(new - old, state.depleted_qty - state.replenished_qty)
            state.displayed_qty = new
            state.last_update_ms = timestamp_ms
        if new == 0:
            book.pop(float(price), None)
        else:
            book[float(price)] = new

        # Critical audit fix: invalidate every pending event whose defended
        # best price has been lost, including when an intermediate update
        # changes the best level before the trade/refill event is processed.
        if side.upper() == "BID" and self.bids:
            current_best = max(self.bids)
            for (s, px), st in self._levels.items():
                if s == "BID" and st.depleted_qty > 0 and current_best < px:
                    st.best_price_preserved = False
        elif side.upper() == "ASK" and self.asks:
            current_best = min(self.asks)
            for (s, px), st in self._levels.items():
                if s == "ASK" and st.depleted_qty > 0 and current_best > px:
                    st.best_price_preserved = False
        return self.snapshot(timestamp_ms)

    def apply_trade(self, timestamp_ms: int, price: float, quantity: float, aggressor_side: str):
        side = aggressor_side.upper()
        if side not in ("BUY", "SELL") or price <= 0 or quantity <= 0:
            raise ValueError("invalid trade")
        book_side = "ASK" if side == "BUY" else "BID"
        key = (book_side, float(price))
        state = self._levels.get(key)
        if state is None or state.depleted_qty < self.config.min_depletion:
            return None
        elapsed = max(1, timestamp_ms - (state.first_depletion_ms or timestamp_ms))
        ratio = state.replenished_qty / state.depleted_qty if state.depleted_qty else 0.0
        confirmed = (state.best_price_preserved and
                     timestamp_ms - (state.first_depletion_ms or timestamp_ms) <= self.config.refill_window_ms and
                     ratio >= self.config.confirmation_ratio)
        result = ReplenishmentResult(book_side, float(price), state.depleted_qty,
                                     state.replenished_qty, ratio,
                                     state.depleted_qty / (elapsed / 1000.0),
                                     confirmed, state.best_price_preserved, timestamp_ms)
        self._results.append(result)
        return result

    def snapshot(self, timestamp_ms: Optional[int] = None):
        ts = self._last_timestamp if timestamp_ms is None else timestamp_ms
        return OrderBookSnapshot(ts or 0, max(self.bids) if self.bids else None,
                                 min(self.asks) if self.asks else None,
                                 dict(self.bids), dict(self.asks))

    @property
    def results(self):
        return tuple(self._results)
