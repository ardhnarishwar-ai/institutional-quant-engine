from datetime import datetime, timezone
import pytest
from adapters.base.events import EventEnvelope, EventType, QuoteEvent, TradeEvent
from adapters.base.instrument import InstrumentId, InstrumentKind
from core.microstructure.analytics_state import AnalyticsRegistry, AnalyticsStateError, InstrumentAnalytics
from core.microstructure.vpin_calculator import VPINConfig

def inst(symbol="RELIANCE"):
    return InstrumentId(venue="NSE", symbol=symbol, kind=InstrumentKind.NSE_SPOT, quote_currency="INR")

_DEFAULT_TIMESTAMP = object()

def trade(i, side="BUY", symbol="RELIANCE", exchange_ns=_DEFAULT_TIMESTAMP,
          receive_ns=_DEFAULT_TIMESTAMP, price=100.0, qty=5.0):
    return EventEnvelope(venue="NSE", instrument=inst(symbol), event_type=EventType.TRADE,
        exchange_timestamp_ns=1_000_000+i if exchange_ns is _DEFAULT_TIMESTAMP else exchange_ns,
        receive_timestamp_ns=2_000_000+i if receive_ns is _DEFAULT_TIMESTAMP else receive_ns,
        sequence_number=i+1, sequence_domain=f"NSE:TRADE:{symbol}",
        payload=TradeEvent(price=price, quantity=qty, aggressor_side=side), event_id=f"{symbol}-t{i}")

def quote(i):
    return EventEnvelope(venue="NSE", instrument=inst(), event_type=EventType.QUOTE,
        exchange_timestamp_ns=1_000_000+i, receive_timestamp_ns=2_000_000+i,
        sequence_number=None, sequence_domain=None, payload=QuoteEvent(last_price=100),
        event_id=f"q{i}")

def make_registry():
    return AnalyticsRegistry(tick_size_by_instrument={inst().canonical_id:0.05, inst("TCS").canonical_id:0.05},
        vpin_config=VPINConfig(volume_bucket_size=10, rolling_buckets=3), profile_rolling_window=3)

def test_trade_updates_all_trade_analytics():
    s=InstrumentAnalytics(venue="NSE", instrument_id=inst().canonical_id, tick_size=0.05,
        vpin_config=VPINConfig(volume_bucket_size=10))
    s.update(trade(0, price=100), reference_level=100)
    r=s.update(trade(1, price=100.01), reference_level=100)
    assert r.classified_trade_count == 2 and r.cvd.cumulative_delta == 10
    assert r.volume_profile.total_volume == 10
    assert r.vpin is not None and r.vpin.completed_buckets == 1
    assert r.absorption is not None and r.audit_sequence == 2

def test_quote_never_becomes_trade():
    s=InstrumentAnalytics(venue="NSE", instrument_id=inst().canonical_id, tick_size=0.05)
    r=s.update(quote(0))
    assert r.ignored_non_trade_count == 1 and r.classified_trade_count == 0
    assert r.cvd is None and r.volume_profile.total_volume == 0 and r.vpin is None

def test_unknown_trade_volume_stays_unknown_and_skips_directional_engines():
    s=InstrumentAnalytics(venue="NSE", instrument_id=inst().canonical_id, tick_size=0.05,
        vpin_config=VPINConfig(volume_bucket_size=10))
    r=s.record_unclassified_trade(event_id="u1", price=100, quantity=7,
        exchange_timestamp_ns=10, receive_timestamp_ns=20)
    assert r.unknown_trade_count == 1 and r.cvd.unknown_volume == 7
    assert r.volume_profile.unknown_volume == 7 and r.vpin is None and r.absorption is None
    assert "UNKNOWN_AGGRESSOR_SIDE_PRESERVED" in r.flags

def test_registry_keeps_instruments_separate():
    reg=make_registry()
    a=reg.update(trade(0, "BUY", "RELIANCE"))
    b=reg.update(trade(0, "SELL", "TCS"))
    assert a.instrument_id != b.instrument_id
    assert a.cvd.buy_volume == 5 and b.cvd.sell_volume == 5
    assert len(reg.snapshots()) == 2

def test_exchange_and_receive_clocks_are_not_substituted():
    s=InstrumentAnalytics(venue="NSE", instrument_id=inst().canonical_id, tick_size=0.05)
    r=s.update(trade(0, exchange_ns=None, receive_ns=987654321))
    assert r.exchange_timestamp_ns is None and r.receive_timestamp_ns == 987654321
    assert r.cvd.exchange_timestamp_ns is None and r.cvd.receive_timestamp_ns == 987654321

def test_duplicate_ids_rejected_on_both_ingest_paths():
    s=InstrumentAnalytics(venue="NSE", instrument_id=inst().canonical_id, tick_size=0.05)
    e=trade(0); s.update(e)
    with pytest.raises(AnalyticsStateError, match="duplicate"): s.update(e)
    with pytest.raises(AnalyticsStateError, match="duplicate"):
        s.record_unclassified_trade(event_id=e.event_id, price=100, quantity=1,
            exchange_timestamp_ns=2_000_000, receive_timestamp_ns=3_000_000)

def test_timestamp_regression_does_not_mutate_state():
    s=InstrumentAnalytics(venue="NSE", instrument_id=inst().canonical_id, tick_size=0.05)
    s.update(trade(1)); before=s.snapshot()
    with pytest.raises(AnalyticsStateError, match="regressed"):
        s.update(trade(2, exchange_ns=1, receive_ns=2_000_001))
    assert s.snapshot() == before

def test_gex_uses_explicit_event_time_and_records_dealer_assumption():
    s=InstrumentAnalytics(venue="NSE", instrument_id=inst().canonical_id, tick_size=0.05)
    s.update(trade(0))
    expiry=datetime(2027,1,1,tzinfo=timezone.utc)
    chain=[dict(strike=100,option_type="C",expiry=expiry,open_interest=100,iv=0.25,
        contract_multiplier=1,dealer_position_sign=-1)]
    g=s.update_gex(chain,spot=100,as_of_timestamp_ns=1_000_000)
    r=s.snapshot()
    assert r.gex == g and r.gex_as_of_timestamp_ns == 1_000_000
    assert "per-row dealer_position_sign" in r.dealer_positioning_assumption

def test_future_gex_asof_rejected():
    s=InstrumentAnalytics(venue="NSE", instrument_id=inst().canonical_id, tick_size=0.05)
    s.update(trade(0, exchange_ns=100, receive_ns=200))
    with pytest.raises(AnalyticsStateError, match="later"):
        s.update_gex([], spot=100, as_of_timestamp_ns=101)

def test_session_reset_clears_session_metrics_but_keeps_audit_counter():
    s=InstrumentAnalytics(venue="NSE", instrument_id=inst().canonical_id, tick_size=0.05)
    s.update(trade(0)); audit=s.snapshot().audit_sequence
    s.start_session("next")
    r=s.snapshot()
    assert r.session_id=="next" and r.event_count==0 and r.cvd is None
    assert r.volume_profile.total_volume==0 and r.audit_sequence==audit
    assert r.vpin is None and r.absorption is None

def test_repeated_replay_is_deterministic():
    def replay():
        reg=make_registry()
        reg.update(trade(0,"BUY","RELIANCE")); reg.update(trade(1,"SELL","RELIANCE"))
        reg.update(trade(0,"SELL","TCS")); reg.update(quote(2))
        return reg.snapshots()
    assert replay()==replay()==replay()

def test_bad_tick_configuration_and_reference_level_rejected():
    reg=AnalyticsRegistry(tick_size_by_instrument={})
    with pytest.raises(AnalyticsStateError, match="tick size"): reg.state_for("NSE",inst().canonical_id)
    s=InstrumentAnalytics(venue="NSE", instrument_id=inst().canonical_id, tick_size=0.05)
    with pytest.raises(ValueError): s.update(trade(0), reference_level=-1)
