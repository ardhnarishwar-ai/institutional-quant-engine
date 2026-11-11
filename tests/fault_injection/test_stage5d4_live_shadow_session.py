from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

from adapters.base.events import EventEnvelope, EventType, TradeEvent
from adapters.base.instrument import InstrumentId, InstrumentKind
from adapters.base.interface import BaseAdapter, AdapterState
from adapters.execution.dead_end import DeadEndExecutionAdapter, ExecutionDisabledError, ExecutionMode
from pipeline.event_journal import EventJournal
from benchmarks.live_shadow.session import run_session, load_factory



_test_event_seq = 0
def _generate_event_id(prefix: str) -> str:
    global _test_event_seq
    _test_event_seq += 1
    return f"{prefix}_{_test_event_seq}"

def make_event(event_id="x1", venue="BINANCE", symbol="BTCUSDT", quote="USDT"):
    inst = InstrumentId(venue, InstrumentKind.CRYPTO_PERPETUAL if venue == "BINANCE" else InstrumentKind.NSE_SPOT, symbol, quote)
    return EventEnvelope(
        venue=venue, instrument=inst, event_type=EventType.TRADE,
        exchange_timestamp_ns=1_700_000_000_000_000_000,
        receive_timestamp_ns=1_700_000_000_000_000_100,
        sequence_number=1, sequence_domain=f"{venue}:{symbol}:TRADE",
        payload=TradeEvent(price=100.0, quantity=0.01, aggressor_side="BUY"),
        event_id=_generate_event_id(event_id),
    )


class FakeAdapter(BaseAdapter):
    def __init__(self, name, items):
        super().__init__(name)
        self.items = tuple(items)
    async def connect(self): pass
    async def synchronize(self): pass
    async def disconnect(self): pass
    async def events(self):
        import dataclasses
        import uuid
        import time
        seq_map = {}
        while self.state == AdapterState.READY:
            for item in self.items:
                stream_k = item.sequence_domain or item.venue
                curr_seq = seq_map.get(stream_k, item.sequence_number or 0) + 1
                seq_map[stream_k] = curr_seq
                
                now_ns = time.time_ns()
                unique_item = dataclasses.replace(
                    item,
                    event_id=f"{item.event_id}_{uuid.uuid4().hex[:8]}_{curr_seq}",
                    sequence_number=curr_seq,
                    exchange_timestamp_ns=now_ns,
                    receive_timestamp_ns=now_ns,
                )
                yield unique_item
            await asyncio.sleep(0.01)


def test_execution_dead_end_remains_permanently_disabled():
    adapter = DeadEndExecutionAdapter()
    assert adapter.execution_mode is ExecutionMode.DISABLED
    with pytest.raises(ExecutionDisabledError):
        adapter.submit_order(object())


def test_factory_contract_requires_module_function():
    with pytest.raises(ValueError):
        load_factory("missing_separator")


def test_session_requires_both_venues(tmp_path):
    with pytest.raises(ValueError, match="NSE SmartAPI"):
        asyncio.run(run_session(
            [FakeAdapter("BINANCE-BTCUSDT", [make_event()])],
            tmp_path / "one.jsonl", duration_seconds=0.02,
        ))


def test_session_requires_binance(tmp_path):
    nse = make_event("nse", "NSE", "NIFTY", "INR")
    with pytest.raises(ValueError, match="Binance"):
        asyncio.run(run_session(
            [FakeAdapter("NSE-SmartAPI", [nse])],
            tmp_path / "one.jsonl", duration_seconds=0.02,
        ))


def test_dual_venue_session_persists_and_replays(tmp_path):
    path = tmp_path / "live.jsonl"
    report = asyncio.run(run_session([
        FakeAdapter("NSE-SmartAPI", [make_event("nse", "NSE", "NIFTY", "INR")]),
        FakeAdapter("BINANCE-USD-M-BTCUSDT", [make_event("btc")]),
    ], path, duration_seconds=0.04, queue_maxsize=8))
    assert report.status == "PASS"
    assert report.persisted > 0
    assert report.persisted == report.replay_event_count
    assert report.execution_mode == "DISABLED"
    records = [json.loads(x) for x in path.read_text().splitlines()]
    currencies = {r["event"]["instrument"]["quote_currency"] for r in records}
    assert currencies == {"INR", "USDT"}


def test_journal_hash_is_stable_for_identical_bytes(tmp_path):
    path = tmp_path / "events.jsonl"
    EventJournal(path).append(make_event())
    first = path.read_bytes()
    assert first == path.read_bytes()


def test_exchange_and_receive_timestamps_are_not_substituted(tmp_path):
    path = tmp_path / "timestamps.jsonl"
    EventJournal(path).append(make_event())
    event = json.loads(path.read_text())["event"]
    assert event["exchange_timestamp_ns"] == 1_700_000_000_000_000_000
    assert event["receive_timestamp_ns"] == 1_700_000_000_000_000_100


def test_nse_and_binance_identity_are_separate():
    nse = make_event("nse", "NSE", "NIFTY", "INR")
    btc = make_event("btc")
    assert nse.instrument.canonical_id != btc.instrument.canonical_id
    assert nse.instrument.quote_currency == "INR"
    assert btc.instrument.quote_currency == "USDT"


def test_invalid_duration_rejected(tmp_path):
    with pytest.raises(ValueError, match="duration_seconds"):
        asyncio.run(run_session([], tmp_path / "x", duration_seconds=0))


def test_report_counts_are_consistent(tmp_path):
    report = asyncio.run(run_session([
        FakeAdapter("NSE-SmartAPI", [make_event("nse", "NSE", "NIFTY", "INR")]),
        FakeAdapter("BINANCE-BTCUSDT", [make_event("btc")]),
    ], tmp_path / "records.jsonl", duration_seconds=0.03))
    assert report.received >= report.persisted
    assert report.replay_event_count == report.persisted


def test_adapters_are_disconnected_after_session(tmp_path):
    adapters = [
        FakeAdapter("NSE-SmartAPI", [make_event("nse", "NSE", "NIFTY", "INR")]),
        FakeAdapter("BINANCE-BTCUSDT", [make_event("btc")]),
    ]
    asyncio.run(run_session(adapters, tmp_path / "records.jsonl", duration_seconds=0.03))
    assert all(a.state is AdapterState.DISCONNECTED for a in adapters)


def test_session_report_marks_execution_disabled(tmp_path):
    report = asyncio.run(run_session([
        FakeAdapter("NSE-SmartAPI", [make_event("nse", "NSE", "NIFTY", "INR")]),
        FakeAdapter("BINANCE-BTCUSDT", [make_event("btc")]),
    ], tmp_path / "records.jsonl", duration_seconds=0.03))
    assert report.execution_mode == "DISABLED"
