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
