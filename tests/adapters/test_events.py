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
