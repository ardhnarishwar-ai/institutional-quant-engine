from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from adapters.base.events import EventEnvelope, EventType, QuoteEvent
from adapters.base.instrument import InstrumentId, InstrumentKind
from adapters.base.interface import AdapterState, BaseAdapter
from pipeline.event_journal import EventJournal
from pipeline.shadow_capture import (
    ReadOnlyShadowCapture,
    ShadowJournalFailure,
)


def event(event_id="e1", symbol="NIFTY", venue="NSE", currency="INR", stamp=100):
    instrument = InstrumentId(
        venue=venue,
        kind=InstrumentKind.NSE_SPOT if venue == "NSE" else InstrumentKind.CRYPTO_PERPETUAL,
        symbol=symbol,
        quote_currency=currency,
    )
    return EventEnvelope(
        venue=venue, instrument=instrument, event_type=EventType.QUOTE,
        exchange_timestamp_ns=stamp, receive_timestamp_ns=stamp + 5,
        sequence_number=1, sequence_domain=f"{venue}:{symbol}:QUOTE",
        payload=QuoteEvent(last_price=100.0), event_id=event_id,
    )


class FakeAdapter(BaseAdapter):
    def __init__(self, name, events=(), *, error=None, gate=None):
        super().__init__(name)
        self.items = list(events)
        self.error = error
        self.gate = gate
        self.disconnect_count = 0

    async def connect(self):
        return None

    async def synchronize(self):
        return None

    async def disconnect(self):
        self.disconnect_count += 1

    async def events(self):
        for item in self.items:
            if self.gate is not None:
                await self.gate.wait()
            yield item
        if self.error:
            raise self.error


class FailingJournal:
    def append(self, event):
        raise OSError("disk full")


def read_records(path):
    import json
    return [json.loads(line) for line in Path(path).read_text().splitlines() if line]


def test_capture_persists_events_and_preserves_both_timestamps(tmp_path):
    path = tmp_path / "shadow.jsonl"
    adapter = FakeAdapter("NSE", [event()])
    capture = ReadOnlyShadowCapture([adapter], journal=EventJournal(path, fsync=True))
    stats = asyncio.run(capture.run())
    record = read_records(path)[0]["event"]
    assert stats.received == stats.persisted == 1
    assert record["exchange_timestamp_ns"] == 100
    assert record["receive_timestamp_ns"] == 105
    assert stats.execution_mode == "DISABLED"


def test_capture_is_bounded_by_configured_queue_capacity(tmp_path):
    async def scenario():
        gate = asyncio.Event()
        adapter = FakeAdapter("NSE", [event(f"e{i}") for i in range(5)], gate=gate)
        capture = ReadOnlyShadowCapture([adapter], journal=EventJournal(tmp_path / "q.jsonl"), queue_maxsize=1)
        await capture.start()
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert capture.queue_depth <= 1
        gate.set()
        await asyncio.gather(*capture._producers)
        await capture._drain_and_close_writer()
        await capture.stop()
        assert capture.stats().persisted == 5
    asyncio.run(scenario())


def test_one_feed_failure_is_reported_without_silent_task_death(tmp_path):
    good = FakeAdapter("GOOD", [event("good")])
    bad = FakeAdapter("BAD", [], error=ConnectionError("feed disconnected"))
    capture = ReadOnlyShadowCapture([good, bad], journal=EventJournal(tmp_path / "fault.jsonl"))
    stats = asyncio.run(capture.run())
    assert stats.persisted == 1
    assert any(x.adapter_name == "BAD" and x.error_type == "ConnectionError" for x in capture.failures)


def test_journal_failure_is_fail_closed(tmp_path):
    adapter = FakeAdapter("NSE", [event()])
    capture = ReadOnlyShadowCapture([adapter], journal=FailingJournal())
    with pytest.raises(ShadowJournalFailure):
        asyncio.run(capture.run())
    assert capture.stats().persisted == 0
    assert capture.fatal_error is not None


def test_startup_rolls_back_previously_started_adapters(tmp_path):
    class Broken(FakeAdapter):
        async def synchronize(self):
            raise RuntimeError("sync failed")
    first = FakeAdapter("FIRST", [])
    second = Broken("SECOND", [])
    capture = ReadOnlyShadowCapture([first, second], journal=EventJournal(tmp_path / "rollback.jsonl"))
    with pytest.raises(RuntimeError, match="sync failed"):
        asyncio.run(capture.start())
    assert first.state == AdapterState.DISCONNECTED
    assert first.disconnect_count == 1


def test_stop_disconnects_started_adapters_and_is_idempotent(tmp_path):
    adapter = FakeAdapter("NSE", [])
    capture = ReadOnlyShadowCapture([adapter], journal=EventJournal(tmp_path / "stop.jsonl"))
    async def scenario():
        await capture.start()
        await capture.stop()
        await capture.stop()
    asyncio.run(scenario())
    assert adapter.state == AdapterState.DISCONNECTED
    assert adapter.disconnect_count == 1


def test_empty_adapter_list_rejected(tmp_path):
    with pytest.raises(ValueError, match="at least one"):
        ReadOnlyShadowCapture([], journal=EventJournal(tmp_path / "empty.jsonl"))


def test_invalid_queue_capacity_rejected(tmp_path):
    for value in (0, -1, True, 1.5):
        with pytest.raises(ValueError):
            ReadOnlyShadowCapture([FakeAdapter("NSE")], journal=EventJournal(tmp_path / "x"), queue_maxsize=value)


def test_nse_and_binance_events_remain_currency_separate(tmp_path):
    nse = event("nse", "NIFTY", "NSE", "INR")
    btc = event("btc", "BTCUSDT", "BINANCE", "USDT")
    path = tmp_path / "mixed.jsonl"
    capture = ReadOnlyShadowCapture(
        [FakeAdapter("NSE", [nse]), FakeAdapter("BINANCE", [btc])],
        journal=EventJournal(path),
    )
    asyncio.run(capture.run())
    currencies = {r["event"]["instrument"]["quote_currency"] for r in read_records(path)}
    assert currencies == {"INR", "USDT"}


def test_execution_boundary_is_disabled_and_not_called(tmp_path):
    from adapters.execution import DeadEndExecutionAdapter
    dead_end = DeadEndExecutionAdapter()
    capture = ReadOnlyShadowCapture([FakeAdapter("NSE", [event()])], journal=EventJournal(tmp_path / "exec.jsonl"))
    stats = asyncio.run(capture.run())
    assert stats.execution_mode == "DISABLED"
    assert dead_end.execution_mode.value == "DISABLED"


def test_capture_writes_only_event_records_not_analytics_or_order_records(tmp_path):
    path = tmp_path / "only-events.jsonl"
    capture = ReadOnlyShadowCapture([FakeAdapter("NSE", [event()])], journal=EventJournal(path))
    asyncio.run(capture.run())
    records = read_records(path)
    assert len(records) == 1
    assert records[0]["record_type"] == "event"


def test_capture_does_not_mutate_event_identity_or_payload(tmp_path):
    original = event("stable-id")
    capture = ReadOnlyShadowCapture([FakeAdapter("NSE", [original])], journal=EventJournal(tmp_path / "identity.jsonl"))
    asyncio.run(capture.run())
    decoded = EventJournal.decode_event(read_records(tmp_path / "identity.jsonl")[0])
    assert decoded.event_id == original.event_id
    assert decoded.payload == original.payload
