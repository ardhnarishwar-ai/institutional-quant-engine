"""End-to-end deterministic replay tests for unified microstructure analytics."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from adapters.base.events import EventEnvelope, EventType, QuoteEvent, TradeEvent
from adapters.base.instrument import InstrumentId, InstrumentKind
from core.microstructure.analytics_state import AnalyticsRegistry
from core.microstructure.vpin_calculator import VPINConfig
from pipeline.analytics_replay import replay_analytics_journals
from pipeline.event_journal import EventJournal


def instrument(symbol="NIFTY"):
    return InstrumentId(
        venue="NSE", symbol=symbol, kind=InstrumentKind.NSE_SPOT,
        quote_currency="INR",
    )


def trade(event_id, seq, exchange_ns, receive_ns, side="BUY", symbol="NIFTY",
          price=100.0, quantity=5.0):
    return EventEnvelope(
        venue="NSE", instrument=instrument(symbol), event_type=EventType.TRADE,
        exchange_timestamp_ns=exchange_ns, receive_timestamp_ns=receive_ns,
        sequence_number=seq, sequence_domain=f"NSE:TRADE:{symbol}",
        payload=TradeEvent(price=price, quantity=quantity, aggressor_side=side),
        event_id=event_id,
    )


def quote(event_id, exchange_ns, receive_ns, symbol="NIFTY"):
    return EventEnvelope(
        venue="NSE", instrument=instrument(symbol), event_type=EventType.QUOTE,
        exchange_timestamp_ns=exchange_ns, receive_timestamp_ns=receive_ns,
        sequence_number=None, sequence_domain=None,
        payload=QuoteEvent(last_price=100.0), event_id=event_id,
    )


def registry():
    return AnalyticsRegistry(
        tick_size_by_instrument={
            instrument("NIFTY").canonical_id: 0.05,
            instrument("BANKNIFTY").canonical_id: 0.05,
        },
        profile_rolling_window=4,
        vpin_config=VPINConfig(volume_bucket_size=10, rolling_buckets=4),
    )


def write_recorded_fixture(path):
    """Create a stable recorded-style journal fixture through the production writer."""
    journal = EventJournal(path)
    journal.append(trade("n1", 1, 1_000, 2_000, "BUY", quantity=6))
    journal.append(trade("n2", 2, 2_000, 3_000, "SELL", quantity=4, price=100.05))
    journal.append(quote("q1", 2_500, 3_500))
    journal.append_metadata({
        "kind": "UNKNOWN_TRADE", "venue": "NSE",
        "instrument_id": instrument().canonical_id, "event_id": "n3-unknown",
        "price": 100.05, "quantity": 7,
        "exchange_timestamp_ns": 3_000, "receive_timestamp_ns": 4_000,
    })
    journal.append(trade("b1", 1, 1_500, 2_500, "SELL", "BANKNIFTY", quantity=3))
    journal.append_metadata({
        "kind": "SESSION_BOUNDARY", "venue": "NSE",
        "instrument_id": instrument().canonical_id, "session_id": "session-2",
    })
    journal.append(trade("n4", 1, 100, 5_000, "BUY", quantity=2))
    return path


def test_recorded_fixture_replay_is_repeatable(tmp_path):
    path = write_recorded_fixture(tmp_path / "recorded_analytics.jsonl")
    first = replay_analytics_journals(path, registry())
    second = replay_analytics_journals(path, registry())
    third = replay_analytics_journals(path, registry())

    assert first == second == third
    assert first.record_count == 7
    assert first.accepted_event_count == 5
    assert first.unknown_trade_count == 1
    assert first.session_boundary_count == 1
    assert first.rejected_record_count == 0

    states = {state.instrument_id: state for state in first.snapshots}
    nifty = states[instrument("NIFTY").canonical_id]
    bank = states[instrument("BANKNIFTY").canonical_id]
    assert nifty.session_id == "session-2"
    assert nifty.cvd.buy_volume == 2
    assert nifty.cvd.sell_volume == 0
    assert nifty.cvd.unknown_volume == 0  # session reset cleared prior session metrics
    assert bank.cvd.sell_volume == 3


def test_unknown_aggressor_volume_is_retained_without_directional_guess(tmp_path):
    path = tmp_path / "unknown_only.jsonl"
    journal = EventJournal(path)
    journal.append_metadata({
        "kind": "UNKNOWN_TRADE", "venue": "NSE",
        "instrument_id": instrument().canonical_id, "event_id": "unknown-1",
        "price": 100.0, "quantity": 9,
        "exchange_timestamp_ns": None, "receive_timestamp_ns": 987_654,
    })

    result = replay_analytics_journals(path, registry())
    state = result.snapshots[0]
    assert result.unknown_trade_count == 1
    assert state.cvd.unknown_volume == 9
    assert state.cvd.buy_volume == 0 and state.cvd.sell_volume == 0
    assert state.exchange_timestamp_ns is None
    assert state.receive_timestamp_ns == 987_654
    assert state.volume_profile.unknown_volume == 9
    assert state.vpin is None and state.absorption is None


def test_duplicate_event_id_is_rejected_and_replay_continues(tmp_path):
    path = tmp_path / "duplicates.jsonl"
    journal = EventJournal(path)
    event = trade("dup-1", 1, 100, 200, "BUY")
    journal.append(event)
    journal.append(event)
    journal.append(trade("after-dup", 2, 200, 300, "SELL"))

    result = replay_analytics_journals(path, registry())
    assert result.accepted_event_count == 2
    assert result.rejected_record_count == 1
    assert "duplicate" in result.issues[0].reason
    state = result.snapshots[0]
    assert state.cvd.buy_volume == 5 and state.cvd.sell_volume == 5


def test_out_of_order_exchange_event_is_rejected_without_state_corruption(tmp_path):
    path = tmp_path / "out_of_order.jsonl"
    journal = EventJournal(path)
    journal.append(trade("first", 1, 200, 300, "BUY"))
    journal.append(trade("late", 2, 150, 400, "SELL"))
    journal.append(trade("last", 3, 250, 500, "SELL"))

    result = replay_analytics_journals(path, registry())
    assert result.accepted_event_count == 2
    assert result.rejected_record_count == 1
    assert "exchange timestamp regressed" in result.issues[0].reason
    state = result.snapshots[0]
    assert state.cvd.buy_volume == 5 and state.cvd.sell_volume == 5
    assert state.event_count == 2


def test_session_boundary_resets_metrics_and_keeps_audit_sequence(tmp_path):
    path = tmp_path / "sessions.jsonl"
    journal = EventJournal(path)
    journal.append(trade("s1", 1, 1_000, 2_000, "BUY", quantity=8))
    journal.append_metadata({
        "kind": "SESSION_BOUNDARY", "venue": "NSE",
        "instrument_id": instrument().canonical_id, "session_id": "next-session",
    })
    journal.append(trade("s2", 1, 10, 3_000, "SELL", quantity=3))

    result = replay_analytics_journals(path, registry())
    state = result.snapshots[0]
    assert result.session_boundary_count == 1
    assert state.session_id == "next-session"
    assert state.cvd.buy_volume == 0 and state.cvd.sell_volume == 3
    assert state.audit_sequence == 2
    assert state.event_count == 1


def test_instrument_streams_remain_isolated_during_replay(tmp_path):
    path = tmp_path / "isolated.jsonl"
    journal = EventJournal(path)
    journal.append(trade("nifty-buy", 1, 100, 200, "BUY", "NIFTY", quantity=11))
    journal.append(trade("bank-sell", 1, 100, 200, "SELL", "BANKNIFTY", quantity=7))

    result = replay_analytics_journals(path, registry())
    states = {s.instrument_id: s for s in result.snapshots}
    assert states[instrument("NIFTY").canonical_id].cvd.cumulative_delta == 11
    assert states[instrument("BANKNIFTY").canonical_id].cvd.cumulative_delta == -7


def test_fail_fast_surfaces_first_bad_record(tmp_path):
    path = tmp_path / "fail_fast.jsonl"
    journal = EventJournal(path)
    event = trade("same", 1, 100, 200)
    journal.append(event)
    journal.append(event)
    with pytest.raises(ValueError, match="duplicate"):
        replay_analytics_journals(path, registry(), fail_fast=True)


def test_exchange_none_survives_recorded_event_journal(tmp_path):
    path = tmp_path / "missing_exchange_clock.jsonl"
    journal = EventJournal(path)
    journal.append(trade("no-exchange-time", 1, None, 123_456, "BUY"))
    result = replay_analytics_journals(path, registry())
    state = result.snapshots[0]
    assert state.exchange_timestamp_ns is None
    assert state.receive_timestamp_ns == 123_456
    assert state.cvd.exchange_timestamp_ns is None
    assert state.cvd.receive_timestamp_ns == 123_456
