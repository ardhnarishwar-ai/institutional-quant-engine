import asyncio
import json

import pytest

from adapters.base.events import (
    DepthDeltaEvent, DepthSnapshotEvent, DepthSnapshotLevel, EventEnvelope,
    EventType, TradeEvent,
)
from adapters.base.instrument import InstrumentId, InstrumentKind
from adapters.replay.clock import ReplayClock
from adapters.replay.ordering import merge_event_streams
from adapters.replay.reader import ReplayReader
from adapters.replay.replay_adapter import ReplayAdapter
from pipeline.event_journal import EventJournal
from pipeline.event_validator import EventValidator, StreamHealth


def nse(symbol="NIFTY"):
    return InstrumentId("NSE", InstrumentKind.NSE_SPOT, symbol)


def crypto():
    return InstrumentId("BINANCE", InstrumentKind.CRYPTO_PERPETUAL,
                        "BTCUSDT", quote_currency="USDT")


def trade(eid, venue_inst, seq, ts, domain):
    return EventEnvelope(
        venue_inst.venue, venue_inst, EventType.TRADE, ts, ts + 10,
        seq, domain, TradeEvent(100, 1, "BUY"), eid)


def depth(eid, seq, ts, typ=EventType.DEPTH_DELTA):
    if typ == EventType.DEPTH_SNAPSHOT:
        payload = DepthSnapshotEvent(
            (DepthSnapshotLevel(99, 2),), (DepthSnapshotLevel(101, 3),), seq)
    else:
        payload = DepthDeltaEvent("BID", 99, 1, "SET")
    return EventEnvelope("NSE", nse(), typ, ts, ts + 1, seq,
                         "nse-depth", payload, eid)


def test_journal_round_trip_preserves_both_timestamps_and_payload(tmp_path):
    path = tmp_path / "events.jsonl"
    event = trade("t1", nse(), 3, 123456789, "nse-trades")
    EventJournal(path).append(event)
    decoded = list(ReplayReader(path).events())
    assert decoded == [event]
    assert decoded[0].exchange_timestamp_ns == 123456789
    assert decoded[0].receive_timestamp_ns == 123456799


def test_journal_metadata_retains_gap_and_recovery(tmp_path):
    path = tmp_path / "events.jsonl"
    journal = EventJournal(path)
    validator = EventValidator(journal=journal)
    validator.validate(depth("d1", 10, 100))
    result = validator.validate(depth("d2", 13, 130))
    assert result.sequence_gap == (11, 12)
    validator.validate(depth("snap", 20, 200, EventType.DEPTH_SNAPSHOT))
    records = list(ReplayReader(path, include_metadata=True).records())
    kinds = [r["metadata"]["kind"] for r in records if r["record_type"] == "metadata"]
    assert "SEQUENCE_GAP" in kinds
    assert "SEQUENCE_RECOVERY" in kinds


def test_gap_quarantines_following_depth_deltas_until_snapshot():
    validator = EventValidator()
    assert validator.validate(depth("d1", 10, 100)).accepted
    gap = validator.validate(depth("d2", 13, 130))
    assert not gap.accepted and gap.quarantined
    assert validator.stream_health(("NSE", nse().canonical_id, "nse-depth")) == StreamHealth.DESYNCHRONIZED
    assert validator.validate(depth("d3", 14, 140)).quarantined
    recovery = validator.validate(depth("snap", 20, 200, EventType.DEPTH_SNAPSHOT))
    assert recovery.accepted and recovery.recovered
    assert validator.stream_health(("NSE", nse().canonical_id, "nse-depth")) == StreamHealth.SYNCHRONIZED
    assert validator.validate(depth("d4", 21, 210)).accepted


def test_sequence_domains_are_isolated_during_gap():
    validator = EventValidator()
    validator.validate(depth("d1", 1, 100))
    validator.validate(depth("d2", 5, 110))
    other = trade("crypto-t1", crypto(), 1, 50, "binance-trades")
    assert validator.validate(other).accepted


def test_snapshot_must_match_envelope_sequence():
    validator = EventValidator()
    bad = EventEnvelope("NSE", nse(), EventType.DEPTH_SNAPSHOT, 1, 2, 8,
                        "nse-depth",
                        DepthSnapshotEvent((DepthSnapshotLevel(99, 1),),
                                           (DepthSnapshotLevel(101, 1),), 9),
                        "bad-snapshot")
    from pipeline.event_validator import EventValidationError
    with pytest.raises(EventValidationError):
        validator.validate(bad)


def test_deterministic_multi_stream_merge():
    nse_events = [trade("n1", nse(), 1, 100, "nse-trades"),
                  trade("n2", nse(), 2, 300, "nse-trades")]
    crypto_events = [trade("c1", crypto(), 1, 100, "binance-trades"),
                     trade("c2", crypto(), 2, 200, "binance-trades")]
    expected = ["c1", "n1", "c2", "n2"]
    for _ in range(5):
        assert [e.event_id for e in merge_event_streams([nse_events, crypto_events])] == expected


def test_replay_adapter_unthrottled_and_ready_lifecycle():
    async def run():
        source = [[trade("a", nse(), 1, 10, "nse-trades"),
                   trade("b", nse(), 2, 20, "nse-trades")]]
        adapter = ReplayAdapter(source, speed="max")
        await adapter.start()
        ids = [e.event_id async for e in adapter.events()]
        await adapter.stop()
        assert ids == ["a", "b"]
    asyncio.run(run())


def test_replay_step_mode_releases_one_event_at_a_time():
    async def run():
        source = [[trade("a", nse(), 1, 10, "nse-trades"),
                   trade("b", nse(), 2, 20, "nse-trades")]]
        adapter = ReplayAdapter(source, step_mode=True)
        await adapter.start()
        iterator = adapter.events()
        first_task = asyncio.create_task(iterator.__anext__())
        await asyncio.sleep(0)
        await adapter.step()
        assert (await first_task).event_id == "a"
        second_task = asyncio.create_task(iterator.__anext__())
        await asyncio.sleep(0)
        await adapter.step()
        assert (await second_task).event_id == "b"
        await adapter.stop()
    asyncio.run(run())


def test_clock_speed_validation():
    with pytest.raises(ValueError):
        ReplayClock(0)
    assert ReplayClock("max").speed == "max"
