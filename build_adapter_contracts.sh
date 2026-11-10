#!/usr/bin/env bash
set -Eeuo pipefail

EXPECTED_REPO="institutional-quant-engine"
EXPECTED_ORIGIN_FRAGMENT="ardhnarishwar-ai/institutional-quant-engine"
BRANCH="main"
COMMIT_MESSAGE="Add canonical adapter contracts and event validation"

ROOT="$(pwd -P)"
if [[ "$(basename "$ROOT")" != "$EXPECTED_REPO" ]]; then
  echo "ERROR: Run from ~/institutional-quant-engine (repository root)." >&2
  echo "Current directory: $ROOT" >&2
  exit 1
fi
if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "ERROR: Not inside a Git repository." >&2
  exit 1
fi
if [[ "$(git branch --show-current)" != "$BRANCH" ]]; then
  echo "ERROR: Expected branch '$BRANCH'. Switch to main first." >&2
  exit 1
fi
ORIGIN="$(git remote get-url origin 2>/dev/null || true)"
if [[ -n "$ORIGIN" && "$ORIGIN" != *"$EXPECTED_ORIGIN_FRAGMENT"* ]]; then
  echo "ERROR: Unexpected origin: $ORIGIN" >&2
  exit 1
fi
if [[ "$(git log -1 --pretty=%s 2>/dev/null || true)" == "$COMMIT_MESSAGE" ]]; then
  echo "Commit 3 is already HEAD; refusing to create a duplicate commit."
  exit 0
fi

# Do not overwrite a user's existing work. The only permitted pre-existing
# untracked file is this update script itself.
while IFS= read -r entry; do
  [[ -z "$entry" ]] && continue
  path="${entry:3}"
  if [[ "$path" != "build_adapter_contracts.sh" ]]; then
    echo "ERROR: Working tree contains pre-existing changes: $entry" >&2
    echo "Commit or stash them before running this script." >&2
    exit 1
  fi
done < <(git status --porcelain)

for target in \
  adapters/base/interface.py \
  adapters/base/events.py \
  adapters/base/instrument.py \
  pipeline/event_validator.py \
  tests/adapters/test_interface.py \
  tests/adapters/test_events.py \
  tests/adapters/test_instrument.py \
  tests/adapters/test_event_validator.py; do
  if [[ -e "$target" ]]; then
    echo "ERROR: Refusing to overwrite existing file: $target" >&2
    exit 1
  fi
done

PYTHON_BIN="$(command -v python3 || command -v python || true)"
if [[ -z "$PYTHON_BIN" ]]; then
  echo "ERROR: Python 3 is required." >&2
  exit 1
fi

mkdir -p adapters/base pipeline tests/adapters
touch adapters/__init__.py adapters/base/__init__.py pipeline/__init__.py tests/__init__.py tests/adapters/__init__.py

cat <<'PYEOF' > adapters/base/instrument.py
"""Canonical, venue-qualified instrument identifiers.

Identifiers are deliberately descriptive and stable; they are not broker
scrip tokens. Venue-specific tokens belong in adapter metadata mappings.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import re


class InstrumentKind(str, Enum):
    NSE_SPOT = "NSE_SPOT"
    NSE_FUTURE = "NSE_FUTURE"
    NSE_OPTION = "NSE_OPTION"
    CRYPTO_PERPETUAL = "CRYPTO_PERPETUAL"


_SYMBOL = re.compile(r"^[A-Z0-9][A-Z0-9._-]{0,31}$")
_CURRENCY = re.compile(r"^[A-Z0-9]{2,12}$")


@dataclass(frozen=True, slots=True)
class InstrumentId:
    venue: str
    kind: InstrumentKind
    symbol: str
    quote_currency: str = "INR"
    expiry: str | None = None
    strike: float | None = None
    option_type: str | None = None

    def __post_init__(self):
        venue = self.venue.strip().upper()
        symbol = self.symbol.strip().upper()
        quote = self.quote_currency.strip().upper()
        if not venue or not re.fullmatch(r"[A-Z0-9_-]{2,24}", venue):
            raise ValueError("venue must be a non-empty uppercase identifier")
        if not _SYMBOL.fullmatch(symbol):
            raise ValueError("invalid instrument symbol")
        if not _CURRENCY.fullmatch(quote):
            raise ValueError("invalid quote_currency")
        object.__setattr__(self, "venue", venue)
        object.__setattr__(self, "symbol", symbol)
        object.__setattr__(self, "quote_currency", quote)

        if self.kind == InstrumentKind.NSE_SPOT:
            if self.expiry is not None or self.strike is not None or self.option_type is not None:
                raise ValueError("NSE spot must not carry derivative fields")
            if quote != "INR":
                raise ValueError("NSE spot quote currency must be INR")
        elif self.kind == InstrumentKind.NSE_FUTURE:
            if not self.expiry or self.strike is not None or self.option_type is not None:
                raise ValueError("NSE future requires expiry and forbids strike/option_type")
            self._validate_expiry(self.expiry)
            if quote != "INR":
                raise ValueError("NSE future quote currency must be INR")
        elif self.kind == InstrumentKind.NSE_OPTION:
            if not self.expiry or self.strike is None or self.strike <= 0:
                raise ValueError("NSE option requires expiry and positive strike")
            if self.option_type not in ("CE", "PE"):
                raise ValueError("NSE option option_type must be CE or PE")
            self._validate_expiry(self.expiry)
            if quote != "INR":
                raise ValueError("NSE option quote currency must be INR")
        elif self.kind == InstrumentKind.CRYPTO_PERPETUAL:
            if self.expiry is not None or self.strike is not None or self.option_type is not None:
                raise ValueError("crypto perpetual must not carry expiry/option fields")
            if not venue or quote == "INR":
                raise ValueError("crypto perpetual requires a crypto quote currency")
        else:
            raise ValueError(f"unsupported instrument kind: {self.kind!r}")

    @staticmethod
    def _validate_expiry(value: str):
        # Canonical expiry representation is YYYY-MM-DD.
        from datetime import date
        try:
            parsed = date.fromisoformat(value)
        except (TypeError, ValueError) as exc:
            raise ValueError("expiry must be YYYY-MM-DD") from exc
        if parsed.isoformat() != value:
            raise ValueError("expiry must be canonical YYYY-MM-DD")

    @property
    def canonical_id(self) -> str:
        base = f"{self.venue}:{self.kind.value}:{self.symbol}:{self.quote_currency}"
        if self.kind == InstrumentKind.NSE_FUTURE:
            return f"{base}:{self.expiry}"
        if self.kind == InstrumentKind.NSE_OPTION:
            return f"{base}:{self.expiry}:{self.strike:g}:{self.option_type}"
        return base
PYEOF

cat <<'PYEOF' > adapters/base/events.py
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
    DEPTH_SNAPSHOT = "DEPTH_SNAPSHOT"
    OPTION_METRIC = "OPTION_METRIC"


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
    snapshot_sequence: int

    def __post_init__(self):
        if not isinstance(self.snapshot_sequence, int) or self.snapshot_sequence < 0:
            raise ValueError("snapshot_sequence must be a non-negative integer")
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


Payload: TypeAlias = TradeEvent | DepthDeltaEvent | DepthSnapshotEvent | OptionMetricEvent
_EXPECTED = {
    EventType.TRADE: TradeEvent,
    EventType.DEPTH_DELTA: DepthDeltaEvent,
    EventType.DEPTH_SNAPSHOT: DepthSnapshotEvent,
    EventType.OPTION_METRIC: OptionMetricEvent,
}


@dataclass(frozen=True, slots=True)
class EventEnvelope:
    venue: str
    instrument: InstrumentId
    event_type: EventType
    exchange_timestamp_ns: int
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
        _nonnegative_int(self.exchange_timestamp_ns, "exchange_timestamp_ns")
        _nonnegative_int(self.receive_timestamp_ns, "receive_timestamp_ns")
        if self.sequence_number is None:
            if self.sequence_domain is not None:
                raise ValueError("sequence_domain requires sequence_number")
        else:
            _nonnegative_int(self.sequence_number, "sequence_number")
            if not isinstance(self.sequence_domain, str) or not self.sequence_domain.strip():
                raise ValueError("sequence_number requires a non-empty sequence_domain")
        if not isinstance(self.payload, _EXPECTED[self.event_type]):
            raise TypeError(f"{self.event_type.value} requires {_EXPECTED[self.event_type].__name__}")
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
PYEOF

cat <<'PYEOF' > adapters/base/interface.py
"""Abstract adapter contract with explicit, guarded lifecycle transitions."""
from __future__ import annotations

from abc import ABC, abstractmethod
from enum import Enum
from threading import RLock
from typing import AsyncIterator

from adapters.base.events import EventEnvelope


class AdapterState(str, Enum):
    DISCONNECTED = "DISCONNECTED"
    CONNECTING = "CONNECTING"
    SYNCING = "SYNCING"
    READY = "READY"
    STALE = "STALE"


_ALLOWED = {
    AdapterState.DISCONNECTED: {AdapterState.CONNECTING},
    AdapterState.CONNECTING: {AdapterState.SYNCING, AdapterState.STALE, AdapterState.DISCONNECTED},
    AdapterState.SYNCING: {AdapterState.READY, AdapterState.STALE, AdapterState.DISCONNECTED},
    AdapterState.READY: {AdapterState.STALE, AdapterState.DISCONNECTED},
    AdapterState.STALE: {AdapterState.SYNCING, AdapterState.DISCONNECTED},
}


class InvalidStateTransition(RuntimeError):
    pass


class BaseAdapter(ABC):
    """Adapters own transport and venue parsing, never core calculations."""

    def __init__(self, adapter_name: str):
        if not adapter_name or not adapter_name.strip():
            raise ValueError("adapter_name is required")
        self.adapter_name = adapter_name
        self._state = AdapterState.DISCONNECTED
        self._state_lock = RLock()

    @property
    def state(self) -> AdapterState:
        with self._state_lock:
            return self._state

    def transition(self, target: AdapterState):
        if not isinstance(target, AdapterState):
            raise TypeError("target must be AdapterState")
        with self._state_lock:
            if target not in _ALLOWED[self._state]:
                raise InvalidStateTransition(f"{self._state.value} -> {target.value} is not allowed")
            self._state = target

    async def start(self):
        if self.state != AdapterState.DISCONNECTED:
            raise InvalidStateTransition("start requires DISCONNECTED state")
        self.transition(AdapterState.CONNECTING)
        try:
            await self.connect()
            self.transition(AdapterState.SYNCING)
            await self.synchronize()
            self.transition(AdapterState.READY)
        except Exception:
            if self.state != AdapterState.DISCONNECTED:
                self.transition(AdapterState.DISCONNECTED)
            raise

    async def mark_stale(self):
        if self.state not in (AdapterState.CONNECTING, AdapterState.SYNCING, AdapterState.READY):
            raise InvalidStateTransition("only active adapters can become STALE")
        self.transition(AdapterState.STALE)

    async def stop(self):
        if self.state == AdapterState.DISCONNECTED:
            return
        await self.disconnect()
        self.transition(AdapterState.DISCONNECTED)

    @abstractmethod
    async def connect(self) -> None:
        """Open the venue transport."""

    @abstractmethod
    async def synchronize(self) -> None:
        """Acquire snapshots and establish a valid sequence boundary."""

    @abstractmethod
    async def disconnect(self) -> None:
        """Close transport and release resources."""

    @abstractmethod
    def events(self) -> AsyncIterator[EventEnvelope]:
        """Yield normalized events while READY; transport-specific implementation."""
PYEOF

cat <<'PYEOF' > pipeline/event_validator.py
"""Per-stream monotonicity, sequence-gap and explicit causal-parent validation."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from adapters.base.events import EventEnvelope


class EventValidationError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class ValidationResult:
    accepted: bool
    sequence_gap: Optional[tuple[int, int]] = None
    causal_parent_pending: bool = False


@dataclass
class _StreamState:
    exchange_timestamp_ns: int
    receive_timestamp_ns: int
    sequence_number: Optional[int]


class EventValidator:
    """Stateful validator.

    Stream identity is (venue, instrument canonical ID, sequence_domain).
    Sequence gaps are reported, not silently repaired. Timestamps must be
    nondecreasing within that same stream. A causal child may arrive before
    its parent; it is flagged pending and rechecked when the parent appears.
    """

    def __init__(self):
        self._streams: dict[tuple, _StreamState] = {}
        self._events: dict[str, EventEnvelope] = {}
        self._pending_children: dict[str, list[EventEnvelope]] = {}

    @staticmethod
    def _stream_key(event: EventEnvelope):
        return (event.venue, event.instrument.canonical_id, event.sequence_domain)

    def validate(self, event: EventEnvelope) -> ValidationResult:
        if not isinstance(event, EventEnvelope):
            raise TypeError("event must be EventEnvelope")
        if event.event_id in self._events:
            raise EventValidationError(f"duplicate event_id: {event.event_id}")

        parent_pending = False
        if event.causal_parent_id:
            parent = self._events.get(event.causal_parent_id)
            if parent is None:
                parent_pending = True
            elif event.exchange_timestamp_ns < parent.exchange_timestamp_ns:
                raise EventValidationError("causal child timestamp precedes parent")

        key = self._stream_key(event)
        previous = self._streams.get(key)
        gap = None
        if previous is not None:
            if event.exchange_timestamp_ns < previous.exchange_timestamp_ns:
                raise EventValidationError("exchange timestamp regressed within stream")
            if event.receive_timestamp_ns < previous.receive_timestamp_ns:
                raise EventValidationError("receive timestamp regressed within stream")
            if event.sequence_number is not None and previous.sequence_number is not None:
                expected = previous.sequence_number + 1
                if event.sequence_number > expected:
                    gap = (expected, event.sequence_number - 1)
                elif event.sequence_number < expected:
                    raise EventValidationError("sequence number regressed or duplicated")
        self._streams[key] = _StreamState(
            event.exchange_timestamp_ns, event.receive_timestamp_ns,
            event.sequence_number,
        )
        self._events[event.event_id] = event

        # If this event is a parent for previously pending children, validate
        # those children now. A violation rejects the parent and rolls back its
        # insertion to avoid retaining a causally inconsistent history.
        children = self._pending_children.pop(event.event_id, [])
        invalid = [child for child in children
                   if child.exchange_timestamp_ns < event.exchange_timestamp_ns]
        if invalid:
            self._events.pop(event.event_id, None)
            self._streams[key] = previous if previous is not None else None
            if previous is None:
                self._streams.pop(key, None)
            raise EventValidationError("previously received causal child precedes this parent")
        if event.causal_parent_id and parent_pending:
            self._pending_children.setdefault(event.causal_parent_id, []).append(event)

        return ValidationResult(True, gap, parent_pending)

    def unresolved_causal_children(self) -> dict[str, tuple[str, ...]]:
        return {parent: tuple(child.event_id for child in children)
                for parent, children in self._pending_children.items()}
PYEOF

cat <<'PYEOF' > tests/adapters/test_events.py
from dataclasses import FrozenInstanceError
import pytest

from adapters.base.events import (
    DepthDeltaEvent, DepthSnapshotEvent, DepthSnapshotLevel, EventEnvelope,
    EventType, OptionMetricEvent, TradeEvent,
)
from adapters.base.instrument import InstrumentId, InstrumentKind


def nse_spot():
    return InstrumentId("NSE", InstrumentKind.NSE_SPOT, "NIFTY")


def envelope(payload, event_type=EventType.TRADE, **kwargs):
    defaults = dict(
        venue="NSE", instrument=nse_spot(), event_type=event_type,
        exchange_timestamp_ns=1_000, receive_timestamp_ns=1_100,
        sequence_number=7, sequence_domain="nse-trades", payload=payload,
        event_id="evt-1",
    )
    defaults.update(kwargs)
    return EventEnvelope(**defaults)


def test_envelope_is_immutable():
    event = envelope(TradeEvent(100, 2, "BUY"))
    with pytest.raises(FrozenInstanceError):
        event.event_id = "changed"


def test_payload_schema_must_match_event_type():
    with pytest.raises(TypeError):
        envelope(DepthDeltaEvent("BID", 100, 2))


def test_sequence_requires_domain():
    with pytest.raises(ValueError):
        envelope(TradeEvent(100, 1, "BUY"), sequence_domain=None)


def test_timestamps_are_integer_nanoseconds():
    with pytest.raises(ValueError):
        envelope(TradeEvent(100, 1, "BUY"), exchange_timestamp_ns=1.5)


def test_trade_payload_validation():
    with pytest.raises(ValueError):
        TradeEvent(100, 0, "BUY")
    with pytest.raises(ValueError):
        TradeEvent(100, 1, "UNKNOWN")


def test_depth_delta_validation():
    with pytest.raises(ValueError):
        DepthDeltaEvent("BID", 100, -1)
    with pytest.raises(ValueError):
        DepthDeltaEvent("ASK", 100, 1, "DELETE")


def test_snapshot_requires_sorted_non_crossed_book():
    with pytest.raises(ValueError):
        DepthSnapshotEvent(
            (DepthSnapshotLevel(99, 1), DepthSnapshotLevel(100, 1)),
            (DepthSnapshotLevel(101, 1),), 10)
    with pytest.raises(ValueError):
        DepthSnapshotEvent(
            (DepthSnapshotLevel(102, 1),),
            (DepthSnapshotLevel(101, 1),), 10)


def test_option_metric_schema():
    with pytest.raises(ValueError):
        OptionMetricEvent(-1, .2, 100)
    with pytest.raises(ValueError):
        OptionMetricEvent(10, .2, 100, 0)


def test_envelope_venue_must_match_instrument():
    with pytest.raises(ValueError):
        envelope(TradeEvent(100, 1, "BUY"), venue="OTHER")
PYEOF

cat <<'PYEOF' > tests/adapters/test_instrument.py
import pytest
from adapters.base.instrument import InstrumentId, InstrumentKind


def test_nse_spot_identifier():
    item = InstrumentId("nse", InstrumentKind.NSE_SPOT, "nifty")
    assert item.canonical_id == "NSE:NSE_SPOT:NIFTY:INR"


def test_nse_future_identifier():
    item = InstrumentId("NSE", InstrumentKind.NSE_FUTURE, "BANKNIFTY",
                        expiry="2026-11-26")
    assert item.canonical_id.endswith(":2026-11-26")


def test_nse_option_identifier():
    item = InstrumentId("NSE", InstrumentKind.NSE_OPTION, "NIFTY",
                        expiry="2026-11-26", strike=25000, option_type="CE")
    assert item.canonical_id.endswith(":2026-11-26:25000:CE")


def test_crypto_perpetual_identifier():
    item = InstrumentId("BINANCE", InstrumentKind.CRYPTO_PERPETUAL,
                        "BTCUSDT", quote_currency="USDT")
    assert "BTCUSDT:USDT" in item.canonical_id


def test_rejects_invalid_derivative_fields():
    with pytest.raises(ValueError):
        InstrumentId("NSE", InstrumentKind.NSE_OPTION, "NIFTY",
                     expiry="2026-11-26", strike=0, option_type="CE")
    with pytest.raises(ValueError):
        InstrumentId("NSE", InstrumentKind.NSE_FUTURE, "NIFTY",
                     expiry="26-11-2026")
PYEOF

cat <<'PYEOF' > tests/adapters/test_interface.py
import pytest
from adapters.base.interface import AdapterState, BaseAdapter, InvalidStateTransition


class DummyAdapter(BaseAdapter):
    def __init__(self, fail_sync=False):
        super().__init__("dummy")
        self.fail_sync = fail_sync

    async def connect(self):
        pass

    async def synchronize(self):
        if self.fail_sync:
            raise RuntimeError("sync failed")

    async def disconnect(self):
        pass

    async def events(self):
        if False:
            yield None


def test_valid_lifecycle():
    import asyncio
    adapter = DummyAdapter()
    asyncio.run(adapter.start())
    assert adapter.state == AdapterState.READY
    asyncio.run(adapter.mark_stale())
    assert adapter.state == AdapterState.STALE
    asyncio.run(adapter.stop())
    assert adapter.state == AdapterState.DISCONNECTED


def test_invalid_transition_rejected():
    adapter = DummyAdapter()
    with pytest.raises(InvalidStateTransition):
        adapter.transition(AdapterState.READY)


def test_failed_sync_returns_to_disconnected():
    import asyncio
    adapter = DummyAdapter(fail_sync=True)
    with pytest.raises(RuntimeError):
        asyncio.run(adapter.start())
    assert adapter.state == AdapterState.DISCONNECTED
PYEOF

cat <<'PYEOF' > tests/adapters/test_event_validator.py
import pytest
from adapters.base.events import EventEnvelope, EventType, TradeEvent
from adapters.base.instrument import InstrumentId, InstrumentKind
from pipeline.event_validator import EventValidationError, EventValidator


def make_event(event_id, seq, exchange_ts, receive_ts, domain="trade-feed",
               instrument="NIFTY", parent=None, venue="NSE"):
    inst = InstrumentId(venue, InstrumentKind.NSE_SPOT, instrument)
    return EventEnvelope(
        venue, inst, EventType.TRADE, exchange_ts, receive_ts, seq, domain,
        TradeEvent(100, 1, "BUY"), event_id, causal_parent_id=parent)


def test_sequence_gap_reported():
    validator = EventValidator()
    validator.validate(make_event("a", 10, 100, 110))
    result = validator.validate(make_event("b", 13, 120, 130))
    assert result.sequence_gap == (11, 12)


def test_sequence_domains_are_isolated():
    validator = EventValidator()
    validator.validate(make_event("nse", 500, 100, 110, "nse-stream"))
    result = validator.validate(make_event("crypto", 2, 50, 60,
                                           "crypto-stream", "BTCUSDT", venue="BINANCE"))
    assert result.sequence_gap is None


def test_instrument_streams_are_isolated():
    validator = EventValidator()
    validator.validate(make_event("nifty", 10, 100, 110, instrument="NIFTY"))
    result = validator.validate(make_event("bank", 2, 50, 60, instrument="BANKNIFTY"))
    assert result.sequence_gap is None


def test_exchange_timestamp_must_be_monotonic():
    validator = EventValidator()
    validator.validate(make_event("a", 10, 100, 110))
    with pytest.raises(EventValidationError):
        validator.validate(make_event("b", 11, 99, 120))


def test_receive_timestamp_must_be_monotonic():
    validator = EventValidator()
    validator.validate(make_event("a", 10, 100, 110))
    with pytest.raises(EventValidationError):
        validator.validate(make_event("b", 11, 120, 109))


def test_sequence_regression_rejected():
    validator = EventValidator()
    validator.validate(make_event("a", 10, 100, 110))
    with pytest.raises(EventValidationError):
        validator.validate(make_event("b", 9, 120, 130))


def test_causal_parent_timestamp_checked():
    validator = EventValidator()
    validator.validate(make_event("parent", 1, 200, 210))
    with pytest.raises(EventValidationError):
        validator.validate(make_event("child", 1, 190, 220,
                                      domain="other-stream", parent="parent"))


def test_child_before_parent_is_pending_then_checked():
    validator = EventValidator()
    result = validator.validate(make_event("child", 1, 220, 230,
                                           domain="child-stream", parent="parent"))
    assert result.causal_parent_pending
    assert "parent" in validator.unresolved_causal_children()
    validator.validate(make_event("parent", 1, 200, 210, domain="parent-stream"))
    assert validator.unresolved_causal_children() == {}
PYEOF

# Run the complete repository test suite. Any failure aborts before commit.
"$PYTHON_BIN" -m pytest -q
# Remove Python and pytest bytecode/cache artifacts before staging, even when
# the target repository has incomplete ignore rules.
find . -type d -name __pycache__ -prune -exec rm -rf {} +
rm -rf .pytest_cache

git add adapters pipeline tests build_adapter_contracts.sh
git commit -m "$COMMIT_MESSAGE"

echo
echo "Commit 3 created locally."
echo "Repository: $ROOT"
echo "Commit: $(git rev-parse --short HEAD)"
echo "Branch: $(git branch --show-current)"
echo
echo "Review and push when ready:"
echo "  git status --short"
echo "  git push origin main"
echo
echo "No exchange connection, API credentials, or live order execution were added."
