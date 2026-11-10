#!/usr/bin/env bash
set -Eeuo pipefail

# Commit 2 hardening update for ardhnarishwar-ai/institutional-quant-engine.
# Run from the repository root: bash harden_core_update.sh
EXPECTED_REPO="institutional-quant-engine"
BRANCH="main"
COMMIT_MESSAGE="Harden microstructure core and add replay validation"

ROOT="$(pwd -P)"
if [[ "$(basename "$ROOT")" != "$EXPECTED_REPO" ]]; then
  echo "ERROR: Run this script from ~/institutional-quant-engine (repository root)." >&2
  echo "Current directory: $ROOT" >&2
  exit 1
fi
if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "ERROR: This directory is not a Git repository." >&2
  exit 1
fi
REMOTE_URL="$(git remote get-url origin 2>/dev/null || true)"
if [[ -n "$REMOTE_URL" && "$REMOTE_URL" != *"ardhnarishwar-ai/institutional-quant-engine"* ]]; then
  echo "ERROR: origin points somewhere unexpected: $REMOTE_URL" >&2
  exit 1
fi
CURRENT_BRANCH="$(git branch --show-current)"
if [[ "$CURRENT_BRANCH" != "$BRANCH" ]]; then
  echo "ERROR: Expected branch '$BRANCH', found '$CURRENT_BRANCH'. Switch to main first." >&2
  exit 1
fi
if [[ "$(git log -1 --pretty=%s 2>/dev/null || true)" == "$COMMIT_MESSAGE" ]]; then
  echo "Commit 2 is already the current HEAD. Refusing to create a duplicate commit."
  exit 0
fi

# Permit only this script itself to be untracked (it is normally copied into
# the repository root immediately before execution). Refuse all other changes.
while IFS= read -r entry; do
  [[ -z "$entry" ]] && continue
  path="${entry:3}"
  if [[ "$path" != "harden_core_update.sh" ]]; then
    echo "ERROR: Working tree has pre-existing changes: $entry" >&2
    echo "Commit/stash them before running this update." >&2
    exit 1
  fi
done < <(git status --porcelain)

if ! command -v python3 >/dev/null 2>&1 && ! command -v python >/dev/null 2>&1; then
  echo "ERROR: Python 3 is required." >&2
  exit 1
fi
PYTHON_BIN="$(command -v python3 || command -v python)"

mkdir -p core/microstructure core/models tests .github/workflows

cat <<'PYEOF' > core/microstructure/orderbook_reconstruction.py
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
PYEOF

cat <<'PYEOF' > core/microstructure/vpin_calculator.py
"""Event-driven VPIN with a prior-only historical reference distribution."""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class VPINConfig:
    volume_bucket_size: float
    rolling_buckets: int = 50
    rank_window: int = 200
    min_rank_observations: int = 20
    volume_tolerance: float = 1e-10

    def __post_init__(self):
        if self.volume_bucket_size <= 0 or self.rolling_buckets < 1:
            raise ValueError("bucket size and rolling_buckets must be positive")
        if self.rank_window < 1 or self.min_rank_observations < 1:
            raise ValueError("rank settings must be positive")
        if self.volume_tolerance <= 0:
            raise ValueError("volume_tolerance must be positive")


@dataclass(frozen=True)
class VPINBucket:
    index: int
    buy_volume: float
    sell_volume: float
    total_volume: float
    imbalance: float
    vpin: float


@dataclass(frozen=True)
class VPINResult:
    timestamp: object
    current_vpin: Optional[float]
    rolling_cdf_toxicity_rank: Optional[float]
    bucket_fill_percentage: float
    current_bucket_buy: float
    current_bucket_sell: float
    current_bucket_imbalance: float
    current_bucket_total: float
    completed_buckets: int
    latest_completed_bucket: Optional[VPINBucket]


class VPINCalculator:
    """A bucket's rank compares its rolling VPIN with prior completed rolling
    VPIN observations only. Percentile uses mid-rank ties:
    (count(reference < x) + 0.5 * count(reference == x)) / N.
    The current observation is appended only after its rank is calculated.
    """

    def __init__(self, config: VPINConfig):
        self.config = config
        self._buy = 0.0
        self._sell = 0.0
        self._completed = deque(maxlen=config.rolling_buckets)
        self._reference = deque(maxlen=config.rank_window)
        self._last_price = None
        self._last_side = None
        self._index = 0
        self._timestamp = None
        self._last_rank = None

    def _classify(self, side, price):
        if side is not None:
            normalized = str(side).upper()
            if normalized not in ("BUY", "SELL"):
                raise ValueError("side must be BUY or SELL")
            return normalized
        if price is None or self._last_price is None:
            raise ValueError("first trade requires explicit BUY/SELL side")
        if price > self._last_price:
            return "BUY"
        if price < self._last_price:
            return "SELL"
        if self._last_side is None:
            raise ValueError("tick-rule fallback has no prior classified side")
        return self._last_side

    @staticmethod
    def _midrank_percentile(value: float, reference) -> float:
        n = len(reference)
        if not n:
            return 0.0
        less = sum(x < value for x in reference)
        equal = sum(x == value for x in reference)
        return (less + 0.5 * equal) / n

    def _complete_bucket(self):
        total = self._buy + self._sell
        imbalance = abs(self._buy - self._sell)
        bucket = VPINBucket(self._index, self._buy, self._sell, total,
                            imbalance, imbalance / self.config.volume_bucket_size)
        self._completed.append(bucket)
        self._index += 1
        rolling = sum(x.vpin for x in self._completed) / len(self._completed)
        if len(self._reference) >= self.config.min_rank_observations:
            self._last_rank = self._midrank_percentile(rolling, tuple(self._reference))
        else:
            self._last_rank = None
        self._reference.append(rolling)
        self._buy = self._sell = 0.0
        return bucket

    def update(self, timestamp, quantity: float, side: Optional[str] = None,
               price: Optional[float] = None) -> VPINResult:
        if quantity <= 0 or (price is not None and price <= 0):
            raise ValueError("quantity and price must be positive")
        classified = self._classify(side, price)
        remaining = float(quantity)
        bucket_size = self.config.volume_bucket_size
        tol = self.config.volume_tolerance
        latest = self._completed[-1] if self._completed else None
        while remaining > tol:
            used = self._buy + self._sell
            capacity = bucket_size - used
            if capacity <= tol:
                latest = self._complete_bucket()
                continue
            take = min(remaining, capacity)
            if classified == "BUY":
                self._buy += take
            else:
                self._sell += take
            remaining -= take
            if bucket_size - (self._buy + self._sell) <= tol:
                latest = self._complete_bucket()
        self._last_side = classified
        if price is not None:
            self._last_price = float(price)
        self._timestamp = timestamp
        current = (sum(b.vpin for b in self._completed) / len(self._completed)
                   if self._completed else None)
        # Rank belongs to the most recently completed bucket; no self-inclusion.
        fill = min(100.0, (self._buy + self._sell) / bucket_size * 100.0)
        return VPINResult(timestamp, current, self._last_rank, fill, self._buy,
                          self._sell, abs(self._buy - self._sell),
                          self._buy + self._sell, self._index, latest)
PYEOF

cat <<'PYEOF' > core/models/market_state.py
"""Thread-safe state aggregation with per-feed sequence and event-time checks.

Sequence numbers are compared only within a shared sequence_domain. Independent
exchange feeds commonly have unrelated sequence spaces and must not be compared.
Each component's own sequence must be strictly increasing when supplied.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from threading import RLock
from typing import Any, Optional


@dataclass(frozen=True)
class MarketStateConfig:
    max_component_age_ms: int = 3000
    max_cross_feed_skew_ms: int = 1000
    max_cross_feed_sequence_skew: int = 0

    def __post_init__(self):
        if min(self.max_component_age_ms, self.max_cross_feed_skew_ms,
               self.max_cross_feed_sequence_skew) < 0:
            raise ValueError("consistency limits cannot be negative")


@dataclass(frozen=True)
class MarketState:
    timestamp: str
    gex: Any = None
    vpin: Any = None
    absorption: Any = None
    orderbook: Any = None
    component_timestamps_ms: dict = field(default_factory=dict)
    component_sequences: dict = field(default_factory=dict)
    component_sequence_domains: dict = field(default_factory=dict)
    stale_components: tuple = ()
    cross_feed_skew_ms: Optional[int] = None
    cross_feed_sequence_skew: Optional[int] = None
    flags: tuple = ()


class MarketStateAggregator:
    COMPONENTS = ("gex", "vpin", "absorption", "orderbook")

    def __init__(self, config: MarketStateConfig = MarketStateConfig()):
        self.config = config
        self._lock = RLock()
        self._values = {name: None for name in self.COMPONENTS}
        self._timestamps = {name: None for name in self.COMPONENTS}
        self._sequences = {name: None for name in self.COMPONENTS}
        self._domains = {name: None for name in self.COMPONENTS}

    def update(self, component: str, value: Any, timestamp_ms: int,
               sequence_number: Optional[int] = None,
               sequence_domain: Optional[str] = None):
        if component not in self.COMPONENTS:
            raise ValueError(f"unknown component: {component}")
        if not isinstance(timestamp_ms, int) or timestamp_ms < 0:
            raise ValueError("timestamp_ms must be a non-negative integer")
        if sequence_number is None and sequence_domain is not None:
            raise ValueError("sequence_domain requires sequence_number")
        if sequence_number is not None:
            if not isinstance(sequence_number, int) or sequence_number < 0:
                raise ValueError("sequence_number must be a non-negative integer")
            if not sequence_domain or not str(sequence_domain).strip():
                raise ValueError("sequence_number requires a non-empty sequence_domain")
        with self._lock:
            previous_ts = self._timestamps[component]
            previous_seq = self._sequences[component]
            if previous_ts is not None and timestamp_ms < previous_ts:
                raise ValueError("component timestamp moved backwards")
            if sequence_number is not None:
                if previous_seq is not None:
                    if sequence_domain != self._domains[component]:
                        raise ValueError("sequence domain cannot change within a component stream")
                    if sequence_number <= previous_seq:
                        raise ValueError("component sequence must increase strictly")
            self._values[component] = value
            self._timestamps[component] = timestamp_ms
            self._sequences[component] = sequence_number
            self._domains[component] = sequence_domain

    def snapshot(self, now_ms: int):
        if not isinstance(now_ms, int) or now_ms < 0:
            raise ValueError("now_ms must be a non-negative integer")
        with self._lock:
            ages = {k: (now_ms - ts if ts is not None else None)
                    for k, ts in self._timestamps.items()}
            stale = tuple(k for k, age in ages.items()
                          if age is None or age > self.config.max_component_age_ms)
            present_ts = [ts for ts in self._timestamps.values() if ts is not None]
            skew = max(present_ts) - min(present_ts) if len(present_ts) >= 2 else None
            flags = []
            if stale:
                flags.append("STALE_COMPONENTS")
            if skew is not None and skew > self.config.max_cross_feed_skew_ms:
                flags.append("CROSS_FEED_SKEW")

            seq_pairs = [(self._domains[k], self._sequences[k])
                         for k in self.COMPONENTS
                         if self._sequences[k] is not None]
            domains = {d for d, _ in seq_pairs}
            sequence_skew = None
            if len(domains) == 1 and len(seq_pairs) >= 2:
                seqs = [s for _, s in seq_pairs]
                sequence_skew = max(seqs) - min(seqs)
                if sequence_skew > self.config.max_cross_feed_sequence_skew:
                    flags.append("CROSS_FEED_SEQUENCE_SKEW")
            elif len(domains) > 1:
                # Sequence values from distinct domains are intentionally incomparable.
                flags.append("MIXED_SEQUENCE_DOMAINS")
            if any(self._sequences[k] is None for k in self.COMPONENTS
                   if self._timestamps[k] is not None):
                flags.append("SEQUENCE_METADATA_MISSING")

            values = {k: (None if k in stale else self._values[k])
                      for k in self.COMPONENTS}
            return MarketState(
                datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc).isoformat(),
                **values,
                component_timestamps_ms=dict(self._timestamps),
                component_sequences=dict(self._sequences),
                component_sequence_domains=dict(self._domains),
                stale_components=stale,
                cross_feed_skew_ms=skew,
                cross_feed_sequence_skew=sequence_skew,
                flags=tuple(flags),
            )

    @staticmethod
    def to_dict(state: MarketState):
        return asdict(state)
PYEOF

cat <<'PYEOF' > tests/test_orderbook_reconstruction.py
import pytest
from core.microstructure.orderbook_reconstruction import OrderBookConfig, OrderBookReconstruction


def make_book(**kwargs):
    book = OrderBookReconstruction(OrderBookConfig(
        refill_window_ms=1000, max_event_age_ms=100,
        min_depletion=1, confirmation_ratio=.5, **kwargs))
    book.apply_depth(0, "BID", 99, 100)
    book.apply_depth(0, "BID", 98, 100)
    book.apply_depth(0, "ASK", 101, 100)
    book.apply_depth(0, "ASK", 102, 100)
    return book


def test_trade_and_depth_reduction_reconcile_then_refill_confirms():
    book = make_book()
    book.apply_trade(100, 99, 40, "SELL")
    book.apply_depth(110, "BID", 99, 60)
    book.apply_depth(150, "BID", 99, 85)
    result = book.apply_trade(160, 99, 1, "SELL")
    assert result is not None
    assert result.depleted_qty == pytest.approx(40)
    assert result.replenished_qty == pytest.approx(25)
    assert result.confirmed


def test_unmatched_depth_reduction_is_not_execution_or_absorption():
    book = make_book()
    book.apply_depth(100, "BID", 99, 50)
    assert book.apply_trade(500, 99, 50, "SELL") is None


def test_max_event_age_rejects_stale_trade_depth_pair():
    book = make_book()
    book.apply_depth(100, "BID", 99, 50)
    assert book.apply_trade(250, 99, 50, "SELL") is None


def test_strict_best_price_loss_invalidates_candidate_permanently():
    book = make_book()
    book.apply_trade(100, 99, 40, "SELL")
    book.apply_depth(110, "BID", 99, 60)
    book.apply_depth(120, "BID", 99, 0)  # best bid becomes 98
    book.apply_depth(130, "BID", 99, 40) # old price returns, still invalid
    result = book.apply_trade(140, 99, 1, "SELL")
    assert result is not None
    assert not result.best_price_preserved
    assert not result.confirmed


def test_rejects_out_of_order_events():
    book = make_book()
    book.apply_depth(100, "BID", 99, 90)
    with pytest.raises(ValueError):
        book.apply_trade(99, 99, 1, "SELL")
PYEOF

cat <<'PYEOF' > tests/test_vpin_calculator.py
import pytest
from core.microstructure.vpin_calculator import VPINCalculator, VPINConfig


def test_exact_bucket_and_vpin():
    calc = VPINCalculator(VPINConfig(volume_bucket_size=10, rolling_buckets=5))
    result = calc.update(1, 10, side="BUY")
    assert result.completed_buckets == 1
    assert result.latest_completed_bucket.vpin == 1.0
    assert result.bucket_fill_percentage == 0


def test_trade_splits_across_multiple_bucket_boundaries():
    calc = VPINCalculator(VPINConfig(volume_bucket_size=10))
    result = calc.update(1, 25, side="BUY")
    assert result.completed_buckets == 2
    assert result.current_bucket_buy == pytest.approx(5)
    assert result.current_bucket_sell == 0


def test_mixed_side_exact_boundary_fixture():
    calc = VPINCalculator(VPINConfig(volume_bucket_size=10, rolling_buckets=3))
    calc.update(1, 6, side="BUY")
    result = calc.update(2, 4, side="SELL")
    assert result.latest_completed_bucket.buy_volume == pytest.approx(6)
    assert result.latest_completed_bucket.sell_volume == pytest.approx(4)
    assert result.latest_completed_bucket.vpin == pytest.approx(.2)


def test_prior_only_midrank_percentile_and_no_self_inclusion():
    calc = VPINCalculator(VPINConfig(
        volume_bucket_size=10, rolling_buckets=1,
        rank_window=10, min_rank_observations=2))
    calc.update(1, 10, side="BUY")  # rolling VPIN 1.0, reference [1.0]
    calc.update(2, 10, side="BUY")  # prior reference has one item: not enough
    result = calc.update(3, 10, side="BUY")  # prior refs [1.0, 1.0]
    assert result.rolling_cdf_toxicity_rank == pytest.approx(.5)


def test_tick_rule_requires_seed_then_carries_direction():
    calc = VPINCalculator(VPINConfig(volume_bucket_size=100))
    with pytest.raises(ValueError):
        calc.update(1, 1, price=100)
    calc.update(2, 1, side="BUY", price=100)
    result = calc.update(3, 1, price=101)
    assert result.current_bucket_buy == 2


def test_invalid_quantity():
    calc = VPINCalculator(VPINConfig(volume_bucket_size=10))
    with pytest.raises(ValueError):
        calc.update(1, 0, side="BUY")
PYEOF

cat <<'PYEOF' > tests/test_market_state.py
import pytest
from core.models.market_state import MarketStateAggregator, MarketStateConfig


def test_stale_components_are_suppressed():
    agg = MarketStateAggregator(MarketStateConfig(max_component_age_ms=100))
    agg.update("gex", {"net": 1}, 1000)
    state = agg.snapshot(1050)
    assert state.gex == {"net": 1}
    assert "vpin" in state.stale_components
    assert state.vpin is None
    assert "STALE_COMPONENTS" in state.flags


def test_cross_feed_timestamp_skew_flag():
    agg = MarketStateAggregator(MarketStateConfig(
        max_component_age_ms=1000, max_cross_feed_skew_ms=50))
    agg.update("gex", 1, 100)
    agg.update("vpin", 2, 200)
    state = agg.snapshot(220)
    assert state.cross_feed_skew_ms == 100
    assert "CROSS_FEED_SKEW" in state.flags


def test_sequence_numbers_must_increase_per_component():
    agg = MarketStateAggregator()
    agg.update("gex", 1, 100, sequence_number=10, sequence_domain="nse-feed")
    with pytest.raises(ValueError):
        agg.update("gex", 2, 101, sequence_number=10, sequence_domain="nse-feed")


def test_same_domain_cross_feed_sequence_alignment():
    agg = MarketStateAggregator(MarketStateConfig(
        max_component_age_ms=1000, max_cross_feed_skew_ms=1000,
        max_cross_feed_sequence_skew=2))
    agg.update("gex", 1, 100, sequence_number=101, sequence_domain="shared")
    agg.update("vpin", 2, 101, sequence_number=105, sequence_domain="shared")
    state = agg.snapshot(110)
    assert state.cross_feed_sequence_skew == 4
    assert "CROSS_FEED_SEQUENCE_SKEW" in state.flags


def test_independent_sequence_domains_are_not_compared():
    agg = MarketStateAggregator()
    agg.update("gex", 1, 100, sequence_number=900, sequence_domain="nse")
    agg.update("vpin", 2, 101, sequence_number=4, sequence_domain="crypto")
    state = agg.snapshot(110)
    assert state.cross_feed_sequence_skew is None
    assert "MIXED_SEQUENCE_DOMAINS" in state.flags


def test_rejects_time_regression():
    agg = MarketStateAggregator()
    agg.update("gex", 1, 100)
    with pytest.raises(ValueError):
        agg.update("gex", 2, 99)
PYEOF

cat <<'PYEOF' > tests/test_event_replay.py
"""Deterministic replay fixtures for core event ordering and state transitions."""
from core.microstructure.orderbook_reconstruction import OrderBookConfig, OrderBookReconstruction
from core.microstructure.vpin_calculator import VPINCalculator, VPINConfig
from core.models.market_state import MarketStateAggregator, MarketStateConfig


def test_deterministic_trade_depth_replay_produces_same_book_result():
    events = [
        ("depth", 0, "BID", 100.0, 50.0),
        ("depth", 0, "BID", 99.0, 50.0),
        ("depth", 0, "ASK", 101.0, 50.0),
        ("trade", 10, 100.0, 20.0, "SELL"),
        ("depth", 11, "BID", 100.0, 30.0),
        ("depth", 20, "BID", 100.0, 42.0),
    ]

    def replay():
        book = OrderBookReconstruction(OrderBookConfig(
            refill_window_ms=100, max_event_age_ms=25,
            min_depletion=1, confirmation_ratio=.5))
        for event in events:
            if event[0] == "depth":
                _, ts, side, price, qty = event
                book.apply_depth(ts, side, price, qty)
            else:
                _, ts, price, qty, aggressor = event
                book.apply_trade(ts, price, qty, aggressor)
        return book.snapshot(20), book._levels[("BID", 100.0)]

    snap_a, state_a = replay()
    snap_b, state_b = replay()
    assert snap_a.bids == snap_b.bids
    assert state_a.executed_depletion_qty == state_b.executed_depletion_qty == 20
    assert state_a.replenished_qty == state_b.replenished_qty == 12


def test_vpin_event_replay_bucket_outputs_are_deterministic():
    events = [(1, 4, "BUY"), (2, 6, "SELL"), (3, 10, "BUY"), (4, 5, "SELL")]
    def replay():
        calc = VPINCalculator(VPINConfig(volume_bucket_size=10, rolling_buckets=4))
        return [calc.update(ts, qty, side=side) for ts, qty, side in events]
    a, b = replay(), replay()
    assert [x.latest_completed_bucket for x in a] == [x.latest_completed_bucket for x in b]
    assert [x.current_vpin for x in a] == [x.current_vpin for x in b]


def test_market_state_replay_rejects_sequence_regression():
    agg = MarketStateAggregator(MarketStateConfig(max_component_age_ms=1000))
    agg.update("gex", 1, 10, sequence_number=10, sequence_domain="feed")
    agg.update("vpin", 2, 11, sequence_number=11, sequence_domain="feed")
    state = agg.snapshot(20)
    assert state.cross_feed_sequence_skew == 1
PYEOF

cat <<'EOF' > .gitignore
__pycache__/
*.py[cod]
.pytest_cache/
.venv/
venv/
.env
.env.*
!.env.example
*.log
.coverage
htmlcov/
dist/
build/
*.egg-info/
.DS_Store
EOF

cat <<'EOF' > pyproject.toml
[build-system]
requires = ["setuptools>=68"]
build-backend = "setuptools.build_meta"

[project]
name = "institutional-quant-engine"
version = "0.2.0"
description = "Exchange-agnostic institutional market microstructure research core"
requires-python = ">=3.10"
dependencies = []

[project.optional-dependencies]
test = ["pytest>=8.3,<9"]

[tool.pytest.ini_options]
testpaths = ["tests"]
addopts = "-ra"
python_files = ["test_*.py"]

[tool.setuptools.packages.find]
include = ["core*"]
EOF

cat <<'EOF' > .github/workflows/tests.yml
name: Core tests

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]
  workflow_dispatch:

permissions:
  contents: read

jobs:
  pytest:
    runs-on: ubuntu-latest
    strategy:
      fail-fast: false
      matrix:
        python-version: ["3.10", "3.11", "3.12"]
    steps:
      - name: Checkout
        uses: actions/checkout@v4
      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: ${{ matrix.python-version }}
      - name: Install test dependencies
        run: python -m pip install --upgrade pip pytest
      - name: Run deterministic test suite
        run: python -m pytest -q
EOF

cat <<'EOF' > requirements.txt
# Runtime calculations use the Python standard library.
# pytest is required for local tests and CI.
pytest>=8.3,<9
EOF

cat <<'EOF' > README.md
# Institutional Quant Engine — microstructure core

Exchange-agnostic research core for observable market microstructure:
- Dealer-sign-conditioned option gamma exposure (GEX)
- Tick-level CVD / price-displacement absorption candidates
- Fixed-volume VPIN buckets and historical toxicity rank
- L2 market-by-price depletion/replenishment reconstruction
- Timestamped aggregate market state with feed consistency checks

## Install and test (Termux/Linux)
```sh
python -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements.txt
python -m pytest -q
```

## Data and model limitations
1. Open interest alone does not reveal dealer inventory. GEX requires an
   externally supplied dealer position sign per option row.
2. Absorption output is a candidate. Aggressor side must be classified
   upstream; L2 replenishment is supporting evidence, not proof of an iceberg.
3. VPIN depends on trade-side classification and bucket size. Tick-rule
   fallback is not the full quote-based Lee–Ready algorithm.
4. L2 market-by-price data cannot identify individual hidden orders.
   Exchange sequence recovery and feed normalization belong in adapters.
5. No broker adapter or live order execution is included or enabled.

## Commit 2: core hardening and deterministic replay
Order-book depth reductions are counted as executed depletion only when
matched to trade prints at the same passive-side price within
`max_event_age_ms`. Unmatched cancellations are not executions. Replenishment
is counted only after matched execution depletion. Losing the defended best
price permanently invalidates that candidate.

VPIN toxicity rank is a prior-only empirical mid-rank percentile of the
current rolling VPIN against historical rolling VPIN observations. The current
observation enters the reference distribution only after its rank is computed.

Market-state aggregation enforces strictly increasing per-component sequence
numbers when supplied. Cross-feed sequence values are compared only when
components explicitly share a `sequence_domain`; unrelated venue sequence
spaces are not compared. Event-time skew is checked independently.

GitHub Actions runs pytest on Python 3.10, 3.11 and 3.12.
EOF

# Run full suite before staging or committing. A failure leaves changes
# uncommitted so the user can inspect the traceback and correct the issue.
"$PYTHON_BIN" -m pytest -q

# Compile all project/test modules as an additional syntax gate.
"$PYTHON_BIN" -m compileall -q core tests

git add \
  core/microstructure/orderbook_reconstruction.py \
  core/microstructure/vpin_calculator.py \
  core/models/market_state.py \
  tests/test_orderbook_reconstruction.py \
  tests/test_vpin_calculator.py \
  tests/test_market_state.py \
  tests/test_event_replay.py \
  .gitignore pyproject.toml .github/workflows/tests.yml \
  requirements.txt README.md harden_core_update.sh

git commit -m "$COMMIT_MESSAGE"

echo
echo "Commit 2 created successfully."
echo "Repository: $ROOT"
echo "Commit: $(git rev-parse --short HEAD)"
echo "Branch: $(git branch --show-current)"
echo
echo "Review and push:"
echo "  git status --short"
echo "  git push origin main"
echo
echo "No exchange feeds or live order execution were configured."
