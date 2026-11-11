from __future__ import annotations

import asyncio
import json

import pytest

from adapters.base.events import EventEnvelope, EventType, TradeEvent
from adapters.base.interface import AdapterState, BaseAdapter
from adapters.base.instrument import InstrumentId, InstrumentKind
from core.microstructure.analytics_state import AnalyticsRegistry
from pipeline.event_journal import EventJournal
from pipeline.event_validator import EventValidator
from pipeline.live_ingest import LiveIngestPipeline, JournalWriteFailure


PROFILES = (
    ("NSE", InstrumentKind.NSE_SPOT, "NIFTY", "INR"),
    ("NSE", InstrumentKind.NSE_SPOT, "BANKNIFTY", "INR"),
    ("NSE", InstrumentKind.NSE_SPOT, "FINNIFTY", "INR"),
    ("BINANCE", InstrumentKind.CRYPTO_PERPETUAL, "BTCUSDT", "USDT"),
)


def make_event(profile, n=1):
    venue, kind, symbol, quote = profile
    instrument = InstrumentId(venue, kind, symbol, quote)
    return EventEnvelope(
        venue=venue, instrument=instrument, event_type=EventType.TRADE,
        exchange_timestamp_ns=1_000 + n, receive_timestamp_ns=2_000 + n,
        sequence_number=n, sequence_domain=f"{venue}:{symbol}:TRADES",
        payload=TradeEvent(100.0 + n, 1.0, "BUY"),
        event_id=f"{symbol}-{n}",
    )


class IterableAdapter(BaseAdapter):
    def __init__(self, name, events, *, fail_after=False, gate=None):
        super().__init__(name)
        self.adapter_name = name
        self.items = list(events)
        self.fail_after = fail_after
        self.gate = gate
        self.disconnected = False

    async def connect(self):
        pass

    async def synchronize(self):
        pass

    async def disconnect(self):
        self.disconnected = True

    async def events(self):
        if self.state != AdapterState.READY:
            raise RuntimeError("not READY")
        for item in self.items:
            if self.gate is not None:
                await self.gate.wait()
            yield item
            await asyncio.sleep(0)
        if self.fail_after:
            raise ConnectionError("injected venue disconnect")


class FailingJournal:
    def append(self, event):
        raise OSError("disk unavailable")

    def append_metadata(self, metadata):
        raise OSError("journal unavailable")


def registry_for(events):
    ticks = {event.instrument.canonical_id: (0.1 if event.instrument.quote_currency == "USDT" else 0.05)
             for event in events}
    return AnalyticsRegistry(tick_size_by_instrument=ticks)


def test_journal_failure_is_fail_closed_and_analytics_never_advances():
    async def run():
        event = make_event(PROFILES[0])
        adapter = IterableAdapter("NSE-feed", [event])
        registry = registry_for([event])
        pipeline = LiveIngestPipeline(
            [adapter], journal=FailingJournal(), validator=EventValidator(), analytics=registry,
        )
        with pytest.raises(JournalWriteFailure):
            await pipeline.run()
        state = registry.state_for(event.venue, event.instrument.canonical_id).snapshot()
        assert state.event_count == 0
        assert pipeline.stats().accepted == 0
        assert pipeline.execution_mode == "DISABLED"
        assert adapter.state == AdapterState.DISCONNECTED
    asyncio.run(run())


def test_one_feed_failure_is_reported_without_silently_killing_other_feed(tmp_path):
    async def run():
        good_event = make_event(PROFILES[3])
        bad = IterableAdapter("NSE-broken", [], fail_after=True)
        good = IterableAdapter("BINANCE-good", [good_event])
        registry = registry_for([good_event])
        pipeline = LiveIngestPipeline(
            [bad, good], journal=EventJournal(tmp_path / "events.jsonl"),
            validator=EventValidator(), analytics=registry,
        )
        await pipeline.run()
        assert pipeline.stats().accepted == 1
        assert pipeline.stats().errors == 1
        assert pipeline.failures[0].adapter_name == "NSE-broken"
        assert good.state == AdapterState.DISCONNECTED
        assert bad.state == AdapterState.DISCONNECTED
        assert registry.state_for("BINANCE", good_event.instrument.canonical_id).snapshot().event_count == 1
    asyncio.run(run())


def test_bounded_queue_never_exceeds_configured_capacity(tmp_path):
    async def run():
        events = [make_event(PROFILES[0], n) for n in range(1, 30)]
        gate = asyncio.Event()
        adapter = IterableAdapter("NSE-feed", events, gate=gate)
        pipeline = LiveIngestPipeline(
            [adapter], journal=EventJournal(tmp_path / "bounded.jsonl"),
            validator=EventValidator(), analytics=registry_for(events),
            queue_maxsize=2,
        )
        await pipeline.start()
        await asyncio.sleep(0.02)
        assert pipeline.queue_depth <= 2
        gate.set()
        await asyncio.gather(*pipeline._producers)
        await pipeline._finish_processor()
        await pipeline.stop()
        assert pipeline.stats().accepted == len(events)
        assert pipeline.queue_depth == 0
    asyncio.run(run())


def test_graceful_stop_drains_already_queued_events(tmp_path):
    async def run():
        events = [make_event(PROFILES[0], n) for n in range(1, 5)]
        gate = asyncio.Event()
        adapter = IterableAdapter("NSE-feed", events, gate=gate)
        pipeline = LiveIngestPipeline(
            [adapter], journal=EventJournal(tmp_path / "drain.jsonl"),
            validator=EventValidator(), analytics=registry_for(events),
            queue_maxsize=8,
        )
        await pipeline.start()
        await asyncio.sleep(0)
        gate.set()
        await asyncio.sleep(0.02)
        await pipeline.stop()
        assert pipeline.stats().accepted == 4
        assert pipeline.queue_depth == 0
        assert adapter.state == AdapterState.DISCONNECTED
    asyncio.run(run())


def test_partial_startup_failure_rolls_back_previously_started_adapters():
    class FailsSync(IterableAdapter):
        async def synchronize(self):
            raise RuntimeError("sync failed")

    async def run():
        first = IterableAdapter("first", [])
        second = FailsSync("second", [])
        pipeline = LiveIngestPipeline(
            [first, second], journal=EventJournal("/tmp/stage5c4_2-startup.jsonl"),
            validator=EventValidator(),
            analytics=AnalyticsRegistry(tick_size_by_instrument={}),
        )
        with pytest.raises(RuntimeError, match="sync failed"):
            await pipeline.start()
        assert first.state == AdapterState.DISCONNECTED
        assert second.state == AdapterState.DISCONNECTED
        assert first.disconnected
    asyncio.run(run())


def test_cancelled_shutdown_still_disconnects_adapters(tmp_path):
    async def run():
        gate = asyncio.Event()
        event = make_event(PROFILES[0])
        adapter = IterableAdapter("blocked", [event], gate=gate)
        pipeline = LiveIngestPipeline(
            [adapter], journal=EventJournal(tmp_path / "cancel.jsonl"),
            validator=EventValidator(), analytics=registry_for([event]),
            shutdown_timeout=0.05,
        )
        await pipeline.start()
        stopper = asyncio.create_task(pipeline.stop())
        await asyncio.sleep(0)
        stopper.cancel()
        await asyncio.gather(stopper, return_exceptions=True)
        # A second stop is idempotent; cleanup is completed by the pipeline.
        await pipeline.stop()
        assert adapter.state == AdapterState.DISCONNECTED
    asyncio.run(run())


@pytest.mark.parametrize("profile", PROFILES)
def test_four_instrument_identity_and_currency_isolation(profile, tmp_path):
    async def run():
        event = make_event(profile)
        adapter = IterableAdapter(profile[2], [event])
        registry = registry_for([event])
        pipeline = LiveIngestPipeline(
            [adapter], journal=EventJournal(tmp_path / f"{profile[2]}.jsonl"),
            validator=EventValidator(), analytics=registry,
        )
        await pipeline.run()
        snapshots = registry.snapshots()
        assert len(snapshots) == 1
        assert snapshots[0].instrument_id == event.instrument.canonical_id
        assert pipeline.execution_mode == "DISABLED"
        rows = [json.loads(line) for line in (tmp_path / f"{profile[2]}.jsonl").read_text().splitlines()]
        assert rows[0]["event"]["instrument"]["quote_currency"] == profile[3]
    asyncio.run(run())
