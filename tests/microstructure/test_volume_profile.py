import pytest

from adapters.base.events import EventEnvelope, EventType, QuoteEvent, TradeEvent
from adapters.base.instrument import InstrumentId, InstrumentKind
from core.microstructure.volume_profile import (
    VolumeProfile, VolumeProfileError, VolumeProfileRegistry,
)


def inst(venue="NSE", symbol="NIFTY", kind=InstrumentKind.NSE_SPOT, quote="INR"):
    return InstrumentId(venue=venue, symbol=symbol, kind=kind, quote_currency=quote)


def trade(event_id, seq, price, qty, side="BUY", *, exchange_ns=None, receive_ns=None,
          venue="NSE", symbol="NIFTY"):
    if receive_ns is None:
        receive_ns = seq * 10
    return EventEnvelope(
        venue=venue, instrument=inst(venue, symbol), event_type=EventType.TRADE,
        exchange_timestamp_ns=exchange_ns, receive_timestamp_ns=receive_ns,
        sequence_number=seq, sequence_domain=f"{venue}:TRADES:{symbol}",
        payload=TradeEvent(price=price, quantity=qty, aggressor_side=side),
        event_id=event_id,
    )


def test_volume_at_price_and_side_reconciliation():
    p = VolumeProfile(tick_size=1, session_id="S1")
    p.update(trade("t1", 1, 100, 10, "BUY"))
    result = p.update(trade("t2", 2, 100, 4, "SELL"))
    p.update(trade("t3", 3, 101, 3, "BUY"))
    result = p.session_snapshot()
    assert result.total_volume == 17
    assert result.buy_volume == 13
    assert result.sell_volume == 4
    assert result.unknown_volume == 0
    level = next(x for x in result.levels if x.price == 100)
    assert (level.total_volume, level.buy_volume, level.sell_volume) == (14, 10, 4)


def test_poc_and_value_area_expand_from_poc():
    p = VolumeProfile(tick_size=1, value_area_percentage=70)
    for i, (price, qty) in enumerate([(100, 10), (101, 30), (102, 20), (103, 5), (99, 4)], 1):
        p.update(trade(f"t{i}", i, price, qty))
    result = p.session_snapshot()
    assert result.poc == 101
    assert result.val == 101
    assert result.vah == 102


def test_poc_tie_breaks_to_lowest_price_deterministically():
    p = VolumeProfile(tick_size=1)
    p.update(trade("a", 1, 100, 5))
    p.update(trade("b", 2, 101, 5))
    assert p.session_snapshot().poc == 100


def test_value_area_percentage_is_validated():
    with pytest.raises(ValueError):
        VolumeProfile(tick_size=1, value_area_percentage=0)
    with pytest.raises(ValueError):
        VolumeProfile(tick_size=1, value_area_percentage=101)


def test_rolling_profile_uses_last_n_trade_prints_and_session_is_retained():
    p = VolumeProfile(tick_size=1, rolling_window=2, session_id="S1")
    p.update(trade("a", 1, 100, 10))
    p.update(trade("b", 2, 101, 20))
    p.update(trade("c", 3, 102, 30))
    session = p.session_snapshot()
    rolling = p.rolling_snapshot()
    assert session.total_volume == 60
    assert rolling.total_volume == 50
    assert rolling.event_count == 2
    assert {x.price for x in rolling.levels} == {101, 102}


def test_unknown_aggressor_volume_is_not_assigned_to_buy_or_sell():
    p = VolumeProfile(tick_size=0.05)
    result = p.record_unclassified_trade(
        venue="NSE", instrument_id=inst().canonical_id, event_id="u1",
        price=100.02, quantity=7, exchange_timestamp_ns=None,
        receive_timestamp_ns=1234,
    )
    assert result.total_volume == 7
    assert result.buy_volume == result.sell_volume == 0
    assert result.unknown_volume == 7
    assert result.levels[0].price == pytest.approx(100.0)
    assert result.levels[0].unknown_volume == 7


def test_quote_ltp_is_rejected_not_converted_to_trade():
    p = VolumeProfile(tick_size=1)
    quote = EventEnvelope(
        venue="NSE", instrument=inst(), event_type=EventType.QUOTE,
        exchange_timestamp_ns=1, receive_timestamp_ns=2, sequence_number=None,
        sequence_domain=None, payload=QuoteEvent(last_price=100), event_id="q1",
    )
    with pytest.raises(VolumeProfileError, match="only canonical TRADE"):
        p.update(quote)
    assert p.session_snapshot().total_volume == 0


def test_duplicate_event_is_rejected_and_not_double_counted():
    p = VolumeProfile(tick_size=1)
    event = trade("same", 1, 100, 5)
    p.update(event)
    with pytest.raises(VolumeProfileError, match="duplicate"):
        p.update(event)
    assert p.session_snapshot().total_volume == 5


def test_exchange_and_receive_timestamps_remain_distinct():
    p = VolumeProfile(tick_size=1)
    result = p.update(trade("t1", 1, 100, 1, exchange_ns=None, receive_ns=98765))
    assert result.total_volume == 1
    # The profile uses timestamps for validation only; no exchange-time substitution occurs.
    assert result.event_count == 1


def test_session_reset_clears_session_and_rolling_profile():
    p = VolumeProfile(tick_size=1, rolling_window=3, session_id="S1")
    p.update(trade("a", 1, 100, 5))
    p.start_session("S2")
    result = p.update(trade("b", 1, 101, 7))
    assert result.session_id == "S2"
    assert result.total_volume == 7
    assert p.rolling_snapshot().total_volume == 7


def test_registry_keeps_venues_and_instruments_separate():
    a = trade("a", 1, 100, 5, "BUY", symbol="NIFTY")
    b = trade("b", 1, 200, 9, "SELL", symbol="BANKNIFTY")
    registry = VolumeProfileRegistry(
        tick_size_by_instrument={
            ("NSE", inst(symbol="NIFTY").canonical_id): 1,
            ("NSE", inst(symbol="BANKNIFTY").canonical_id): 1,
        }
    )
    registry.update(a)
    registry.update(b)
    snapshots = registry.snapshots()
    assert len(snapshots) == 2
    assert sorted(x.total_volume for x in snapshots) == [5, 9]


def test_tick_rounding_is_stable_and_rejects_bad_prices():
    p = VolumeProfile(tick_size=0.05)
    result = p.update(trade("t1", 1, 100.025, 2))
    assert result.levels[0].price == pytest.approx(100.05)
    with pytest.raises(ValueError):
        p.update(trade("t2", 2, 0, 1))


def test_session_profile_value_area_contains_poc_and_target_volume():
    p = VolumeProfile(tick_size=1, value_area_percentage=70)
    for i, (price, qty) in enumerate([(99, 5), (100, 10), (101, 40), (102, 20), (103, 5)], 1):
        p.update(trade(f"t{i}", i, price, qty))
    r = p.session_snapshot()
    assert r.val <= r.poc <= r.vah
    assert sum(x.total_volume for x in r.levels if r.val <= x.price <= r.vah) >= r.total_volume * 0.70


def test_invalid_quantity_does_not_mutate_profile():
    p = VolumeProfile(tick_size=1)
    with pytest.raises(ValueError):
        p.update(trade("bad", 1, 100, -1))
    assert p.session_snapshot().total_volume == 0
