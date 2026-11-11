from __future__ import annotations

import asyncio
import json

import pytest

from adapters.base.events import EventEnvelope, EventType, TradeEvent
from adapters.base.instrument import InstrumentId, InstrumentKind
from adapters.base.interface import AdapterState, BaseAdapter
from adapters.crypto.binance_usdm.adapter import BinanceUSDMLiveAdapter
from pipeline.shadow_capture import ReadOnlyShadowCapture, ShadowJournalFailure


def trade_frame(aid: int) -> str:
    return json.dumps({
        "e": "aggTrade", "E": 1700000000000 + aid,
        "T": 1700000000000 + aid, "s": "BTCUSDT",
        "a": aid, "p": "50000.0", "q": "0.001", "m": False,
    })


def test_binance_queue_is_bounded_by_configured_capacity():
    adapter = BinanceUSDMLiveAdapter(queue_maxsize=3)
    assert adapter._queue.maxsize == 3
    assert adapter.queue_maxsize == 3


@pytest.mark.parametrize("value", [0, -1, True, 1.5, "4"])
def test_binance_queue_capacity_rejects_invalid_values(value):
    with pytest.raises(ValueError, match="queue_maxsize"):
        BinanceUSDMLiveAdapter(queue_maxsize=value)


def test_binance_ingest_applies_backpressure_without_dropping():
    async def scenario():
        adapter = BinanceUSDMLiveAdapter(queue_maxsize=1)
        await adapter.ingest(trade_frame(1))
        assert adapter._queue.qsize() == 1
        blocked = asyncio.create_task(adapter.ingest(trade_frame(2)))
        await asyncio.sleep(0)
        assert not blocked.done()
        first = await adapter._queue.get()
        adapter._queue.task_done()
        await asyncio.wait_for(blocked, timeout=1)
        second = await adapter._queue.get()
        adapter._queue.task_done()
        assert first.sequence_number == 1
        assert second.sequence_number == 2
    asyncio.run(scenario())


def test_binance_disconnect_does_not_silently_clear_queued_events():
    async def scenario():
        adapter = BinanceUSDMLiveAdapter(queue_maxsize=2)
        await adapter.ingest(trade_frame(1))
        await adapter.stop()
        assert adapter._queue.qsize() == 1
        event = await adapter._queue.get()
        assert event.sequence_number == 1
    asyncio.run(scenario())


class OneEventAdapter(BaseAdapter):
    def __init__(self):
        super().__init__("NSE-SMARTAPI-TEST")
        self.disconnected = False
        self._event = EventEnvelope(
            venue="NSE",
            instrument=InstrumentId("NSE", InstrumentKind.NSE_SPOT, "NIFTY", "INR"),
            event_type=EventType.TRADE,
            exchange_timestamp_ns=1_700_000_000_000_000_000,
            receive_timestamp_ns=1_700_000_000_000_000_100,
            sequence_number=1,
            sequence_domain="NSE:NIFTY:TRADE",
            payload=TradeEvent(22000.0, 1.0, "BUY"),
            event_id="nse-test-1",
        )

    async def connect(self): pass
    async def synchronize(self): pass
    async def disconnect(self): self.disconnected = True
    async def events(self):
        while self.state == AdapterState.READY:
            yield self._event
            await asyncio.sleep(0)


class FailingJournal:
    def append(self, event):
        raise OSError("simulated durable storage failure")


def test_journal_failure_cancels_blocked_producers_and_disconnects_adapters():
    async def scenario():
        adapter = OneEventAdapter()
        capture = ReadOnlyShadowCapture(
            [adapter], journal=FailingJournal(), queue_maxsize=1,
            shutdown_timeout=0.5,
        )
        await capture.start()
        for _ in range(100):
            if capture.fatal_error is not None:
                break
            await asyncio.sleep(0.005)
        assert capture.fatal_error is not None
        await capture.stop()
        assert adapter.state is AdapterState.DISCONNECTED
        assert adapter.disconnected
        assert capture.stats().persisted == 0
        assert any(item.adapter_name == "SHADOW_JOURNAL" for item in capture.failures)
    asyncio.run(scenario())


def test_journal_failure_is_fail_closed_and_never_counts_unwritten_event():
    async def scenario():
        adapter = OneEventAdapter()
        capture = ReadOnlyShadowCapture([adapter], journal=FailingJournal(), queue_maxsize=1)
        with pytest.raises(ShadowJournalFailure):
            await capture.run()
        assert capture.stats().persisted == 0
        assert adapter.state is AdapterState.DISCONNECTED
    asyncio.run(scenario())
