from dataclasses import replace

import pytest

from adapters.base.events import EventEnvelope, EventType, QuoteEvent, TradeEvent
from adapters.base.instrument import InstrumentId, InstrumentKind
from core.microstructure.cvd_calculator import (
    CVDCalculator, CVDRegistry, CVDValidationError, verify_cvd_replay,
)


def instrument(venue="NSE", symbol="NIFTY"):
    return InstrumentId(venue=venue, symbol=symbol, kind=InstrumentKind.NSE_SPOT, quote_currency="INR")


def trade(event_id, seq, side, qty, *, exchange_ns=None, receive_ns=None, venue="NSE", symbol="NIFTY"):
    if receive_ns is None:
        receive_ns = seq * 10
    return EventEnvelope(
        venue=venue, instrument=instrument(venue, symbol), event_type=EventType.TRADE,
        exchange_timestamp_ns=exchange_ns, receive_timestamp_ns=receive_ns,
        sequence_number=seq, sequence_domain=f"{venue}:TRADES:{symbol}",
        payload=TradeEvent(price=100.0, quantity=qty, aggressor_side=side),
        event_id=event_id,
    )


def quote(event_id="q1"):
    return EventEnvelope(
        venue="NSE", instrument=instrument(), event_type=EventType.QUOTE,
        exchange_timestamp_ns=100, receive_timestamp_ns=100,
        sequence_number=None, sequence_domain=None, payload=QuoteEvent(last_price=100),
        event_id=event_id,
    )


def test_buy_sell_delta_and_cumulative_delta():
    calc = CVDCalculator()
    a = calc.update(trade("t1", 1, "BUY", 10))
    b = calc.update(trade("t2", 2, "SELL", 4))
    assert (a.delta, a.cumulative_delta) == (10, 10)
    assert (b.buy_volume, b.sell_volume, b.delta) == (10, 4, -4)
    assert b.cumulative_delta == 6
    assert b.total_observed_volume == 14
    assert b.classified_delta_ratio == pytest.approx(6 / 14)


def test_rolling_delta_is_bounded_by_trade_count():
    calc = CVDCalculator(rolling_window=2)
    calc.update(trade("t1", 1, "BUY", 5))
    calc.update(trade("t2", 2, "SELL", 2))
    result = calc.update(trade("t3", 3, "BUY", 7))
    assert result.cumulative_delta == 10
    assert result.rolling_delta == 5  # last two deltas: -2 + 7


def test_quotes_and_ltp_are_never_converted_to_trades():
    calc = CVDCalculator()
    with pytest.raises(CVDValidationError, match="only canonical TRADE"):
        calc.update(quote())
    assert calc.snapshot is None


def test_unclassified_volume_remains_unknown_and_does_not_change_cvd():
    calc = CVDCalculator()
    calc.update(trade("t1", 1, "BUY", 8))
    result = calc.record_unclassified_trade(
        event_id="unknown-1", exchange_timestamp_ns=None,
        receive_timestamp_ns=20, quantity=5,
    )
    assert result.buy_volume == 8
    assert result.sell_volume == 0
    assert result.unknown_volume == 5
    assert result.total_observed_volume == 13
    assert result.cumulative_delta == 8
    assert result.delta == 0
    assert result.exchange_timestamp_ns is None


def test_exchange_timestamp_none_is_not_replaced_by_receive_time():
    calc = CVDCalculator()
    result = calc.update(trade("t1", 1, "BUY", 2, exchange_ns=None, receive_ns=987654))
    assert result.exchange_timestamp_ns is None
    assert result.receive_timestamp_ns == 987654


def test_duplicate_event_id_is_rejected_without_double_counting():
    calc = CVDCalculator()
    event = trade("same", 1, "BUY", 3)
    calc.update(event)
    with pytest.raises(CVDValidationError, match="duplicate"):
        calc.update(event)
    assert calc.snapshot.buy_volume == 3


def test_out_of_order_receive_timestamp_is_rejected():
    calc = CVDCalculator()
    calc.update(trade("t1", 1, "BUY", 1, receive_ns=100))
    with pytest.raises(CVDValidationError, match="receive timestamp regressed"):
        calc.update(trade("t2", 2, "SELL", 1, receive_ns=99))


def test_exchange_timestamp_regression_is_rejected():
    calc = CVDCalculator()
    calc.update(trade("t1", 1, "BUY", 1, exchange_ns=200, receive_ns=10))
    with pytest.raises(CVDValidationError, match="exchange timestamp regressed"):
        calc.update(trade("t2", 2, "SELL", 1, exchange_ns=199, receive_ns=20))


def test_instrument_scoping_prevents_cross_instrument_contamination():
    calc = CVDCalculator()
    calc.update(trade("t1", 1, "BUY", 1))
    with pytest.raises(CVDValidationError, match="instrument-scoped"):
        calc.update(trade("t2", 2, "SELL", 1, symbol="BANKNIFTY"))


def test_registry_keeps_instruments_independent():
    registry = CVDRegistry()
    a = registry.update(trade("t1", 1, "BUY", 10, symbol="NIFTY"))
    b = registry.update(trade("t2", 1, "SELL", 7, symbol="BANKNIFTY"))
    assert a.cumulative_delta == 10
    assert b.cumulative_delta == -7
    assert len(registry.snapshots()) == 2


def test_session_reset_clears_session_totals_and_rolling_window():
    calc = CVDCalculator(rolling_window=2, session_id="2026-11-10")
    calc.update(trade("t1", 1, "BUY", 10))
    calc.start_session("2026-11-11")
    result = calc.update(trade("t2", 1, "SELL", 3))
    assert result.session_id == "2026-11-11"
    assert result.buy_volume == 0
    assert result.sell_volume == 3
    assert result.cumulative_delta == -3
    assert result.rolling_delta == -3


def test_replay_is_deterministic_and_ignores_quotes():
    events = [
        trade("t1", 1, "BUY", 10, exchange_ns=100),
        quote("q1"),
        trade("t2", 2, "SELL", 4, exchange_ns=200),
    ]
    first = verify_cvd_replay(events, rolling_window=10)
    second = verify_cvd_replay(iter(events), rolling_window=10)
    assert first == second
    assert first.event_count == 3
    assert first.classified_trade_count == 2
    assert first.ignored_non_trade_count == 1
    assert first.final_results[0].cumulative_delta == 6


def test_replay_rejects_duplicate_trade_ids():
    events = [trade("t1", 1, "BUY", 1), trade("t1", 2, "SELL", 2)]
    with pytest.raises(CVDValidationError, match="duplicate"):
        verify_cvd_replay(events)


def test_unknown_trade_does_not_create_directional_delta():
    calc = CVDCalculator()
    result = calc.record_unclassified_trade(
        event_id="u1", exchange_timestamp_ns=100, receive_timestamp_ns=110, quantity=12
    )
    assert result.unknown_volume == 12
    assert result.buy_volume == result.sell_volume == result.cumulative_delta == 0
    assert result.classified_delta_ratio is None
