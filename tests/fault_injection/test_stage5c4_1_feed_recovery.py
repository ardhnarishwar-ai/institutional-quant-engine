from __future__ import annotations

import asyncio
import json

import pytest

from adapters.base.events import (
    DepthDeltaEvent,
    DepthSnapshotEvent,
    DepthSnapshotLevel,
    EventEnvelope,
    EventType,
    TradeEvent,
)
from adapters.base.interface import AdapterState, BaseAdapter
from adapters.base.instrument import InstrumentId, InstrumentKind
from adapters.crypto.binance_usdm.adapter import BinanceUSDMLiveAdapter
from adapters.nse.smartapi.feed_health import FeedHealthMonitor, FeedStatus
from adapters.nse.smartapi.feed_supervisor import BackoffPolicy, FeedSupervisor
from core.microstructure.analytics_state import AnalyticsRegistry
from pipeline.depth_recovery import DepthHealth, DepthRecoveryManager
from pipeline.event_validator import EventValidationError, EventValidator
from pipeline.event_journal import EventJournal
from pipeline.snapshot_sync import SnapshotSyncCoordinator


PROFILES = (
    ("NSE", InstrumentKind.NSE_SPOT, "NIFTY", "INR", "NSE:NIFTY:DEPTH"),
    ("NSE", InstrumentKind.NSE_SPOT, "BANKNIFTY", "INR", "NSE:BANKNIFTY:DEPTH"),
    ("NSE", InstrumentKind.NSE_SPOT, "FINNIFTY", "INR", "NSE:FINNIFTY:DEPTH"),
    ("BINANCE", InstrumentKind.CRYPTO_PERPETUAL, "BTCUSDT", "USDT", "BINANCE:BTCUSDT:DEPTH"),
)


def instrument(profile):
    venue, kind, symbol, quote, _ = profile
    return InstrumentId(venue, kind, symbol, quote)


def trade_event(profile, *, seq=1, event_id="evt-1", exchange_ns=1_000,
                receive_ns=2_000, domain=None):
    venue, _, _, _, default_domain = profile
    ins = instrument(profile)
    return EventEnvelope(
        venue=venue,
        instrument=ins,
        event_type=EventType.TRADE,
        exchange_timestamp_ns=exchange_ns,
        receive_timestamp_ns=receive_ns,
        sequence_number=seq,
        sequence_domain=domain or default_domain.replace("DEPTH", "TRADE"),
        payload=TradeEvent(100.0, 1.0, "BUY"),
        event_id=event_id,
    )


def snapshot(sequence=100, bid=99.0, ask=101.0):
    return DepthSnapshotEvent(
        bids=(DepthSnapshotLevel(bid, 10.0),),
        asks=(DepthSnapshotLevel(ask, 12.0),),
        snapshot_sequence=sequence,
    )


def test_depth_gap_quarantines_updates_until_fresh_snapshot_for_all_four_instruments(tmp_path):
    journal = EventJournal(tmp_path / "depth-recovery.jsonl")
    manager = DepthRecoveryManager(journal=journal)

    for profile in PROFILES:
        venue, _, _, _, domain = profile
        iid = instrument(profile).canonical_id
        assert manager.apply_snapshot(venue, iid, domain, snapshot(100)).accepted
        assert manager.apply_delta(
            venue, iid, domain, DepthDeltaEvent("BID", 99.0, 11.0),
            sequence_number=101,
        ).accepted

        gap = manager.apply_delta(
            venue, iid, domain, DepthDeltaEvent("ASK", 102.0, 2.0),
            sequence_number=103,
        )
        assert not gap.accepted
        assert gap.health == DepthHealth.DESYNCHRONIZED
        assert gap.sequence_gap == (102, 102)
        assert manager.book(venue, iid, domain) is None

        quarantined = manager.apply_delta(
            venue, iid, domain, DepthDeltaEvent("BID", 98.0, 1.0),
            sequence_number=104,
        )
        assert not quarantined.accepted
        assert quarantined.reason == "awaiting_recovery_snapshot"

        recovered = manager.apply_snapshot(venue, iid, domain, snapshot(200, 98.0, 102.0))
        assert recovered.accepted and recovered.recovered
        assert manager.book(venue, iid, domain)["last_sequence"] == 200
        assert manager.health(venue, iid, domain) == DepthHealth.SYNCHRONIZED

    records = (tmp_path / "depth-recovery.jsonl").read_text().splitlines()
    assert sum("DEPTH_SEQUENCE_GAP" in row for row in records) == 4
    assert sum("DEPTH_SNAPSHOT_RECOVERY" in row for row in records) == 4


def test_depth_sequence_domains_are_isolated_during_recovery():
    profile = PROFILES[0]
    venue, _, _, _, _ = profile
    iid = instrument(profile).canonical_id
    manager = DepthRecoveryManager()
    manager.apply_snapshot(venue, iid, "NSE:DEPTH:A", snapshot(10))
    manager.apply_snapshot(venue, iid, "NSE:DEPTH:B", snapshot(50))

    broken = manager.apply_delta(
        venue, iid, "NSE:DEPTH:A", DepthDeltaEvent("BID", 99.0, 1.0),
        sequence_number=12,
    )
    assert not broken.accepted
    assert manager.health(venue, iid, "NSE:DEPTH:A") == DepthHealth.DESYNCHRONIZED
    assert manager.health(venue, iid, "NSE:DEPTH:B") == DepthHealth.SYNCHRONIZED
    assert manager.book(venue, iid, "NSE:DEPTH:B")["last_sequence"] == 50


def test_feed_health_stale_timeout_uses_monotonic_clock_not_event_clocks():
    monitor = FeedHealthMonitor(stale_after_ns=10)
    event = trade_event(PROFILES[0], exchange_ns=1_000_000, receive_ns=9_000_000)
    healthy = monitor.observe(event, monotonic_ns=100)
    assert healthy.status == FeedStatus.HEALTHY
    assert healthy.latency_ns == 8_000_000

    stale = monitor.snapshot(
        event.venue, event.instrument.canonical_id, event.sequence_domain,
        monotonic_ns=111,
    )
    assert stale.status == FeedStatus.STALE
    assert stale.stale_for_ns == 11
    assert stale.last_exchange_timestamp_ns == 1_000_000
    assert stale.last_receive_timestamp_ns == 9_000_000


def test_feed_health_marks_duplicate_and_sequence_regression_desynchronized():
    monitor = FeedHealthMonitor(stale_after_ns=1_000_000)
    first = trade_event(PROFILES[0], seq=10, event_id="first", exchange_ns=100, receive_ns=200)
    duplicate = trade_event(PROFILES[0], seq=10, event_id="duplicate", exchange_ns=101, receive_ns=201)
    regressed = trade_event(PROFILES[0], seq=9, event_id="regressed", exchange_ns=102, receive_ns=202)

    assert monitor.observe(first, monotonic_ns=1).status == FeedStatus.HEALTHY
    assert monitor.observe(duplicate, monotonic_ns=2).status == FeedStatus.DESYNCHRONIZED
    final = monitor.observe(regressed, monotonic_ns=3)
    assert final.status == FeedStatus.DESYNCHRONIZED
    assert final.sequence_gap == (9, 10)


def test_event_validator_rejects_duplicate_event_ids_and_out_of_order_sequences():
    validator = EventValidator()
    first = trade_event(PROFILES[0], seq=10, event_id="same", exchange_ns=100, receive_ns=200)
    assert validator.validate(first).accepted
    with pytest.raises(EventValidationError, match="duplicate event_id"):
        validator.validate(first)

    validator2 = EventValidator()
    assert validator2.validate(first).accepted
    late = trade_event(PROFILES[0], seq=9, event_id="late", exchange_ns=101, receive_ns=201)
    with pytest.raises(EventValidationError, match="regressed or duplicated"):
        validator2.validate(late)


def test_unsequenced_nse_mode4_snapshot_does_not_fabricate_sequence(tmp_path):
    profile = PROFILES[0]
    venue, _, _, _, _ = profile
    ins = instrument(profile)
    event = EventEnvelope(
        venue=venue, instrument=ins, event_type=EventType.DEPTH_SNAPSHOT,
        exchange_timestamp_ns=None, receive_timestamp_ns=123_456,
        sequence_number=None, sequence_domain=None,
        payload=snapshot(None), event_id="nse-mode4-unsequenced",
    )
    manager = DepthRecoveryManager()
    journal = EventJournal(tmp_path / "mode4.jsonl")
    status = SnapshotSyncCoordinator(manager, journal).apply(event)
    assert status.snapshot_sequence is None
    assert not status.sequence_tracked
    book = manager.book(venue, ins.canonical_id, "NSE:UNSEQUENCED_DEPTH")
    assert book is not None and book["last_sequence"] is None


def test_binance_depth_gap_requests_snapshot_recovery_without_clock_substitution():
    async def run():
        calls = []

        async def snapshot_provider(symbol, limit):
            calls.append((symbol, limit))
            return {
                "lastUpdateId": 100 if len(calls) == 1 else 200,
                "bids": [["34999", "2"]],
                "asks": [["35001", "3"]],
            }

        class FakeTransport:
            def __init__(self):
                self.connected = False
            async def connect(self):
                self.connected = True
            async def recv(self):
                await asyncio.Future()
            async def disconnect(self):
                self.connected = False

        adapter = BinanceUSDMLiveAdapter(
            transport=FakeTransport(),
            snapshot_provider=snapshot_provider,
            receive_clock=lambda: 1_800_000_000_000_000_000,
            monotonic_clock=lambda: 50,
        )
        await adapter.start()
        initial = await adapter._queue.get()
        assert initial.event_type == EventType.DEPTH_SNAPSHOT
        assert initial.exchange_timestamp_ns is None

        first = {
            "e": "depthUpdate", "E": 1_700_000_000_001, "T": 1_700_000_000_000,
            "s": "BTCUSDT", "U": 101, "u": 102, "pu": 99,
            "b": [["35000", "1"]], "a": [["35001", "2"]],
        }
        gap = {
            "e": "depthUpdate", "E": 1_700_000_000_002, "T": 1_700_000_000_001,
            "s": "BTCUSDT", "U": 110, "u": 111, "pu": 109,
            "b": [["35000", "0"]], "a": [["35001", "2"]],
        }
        await adapter.ingest(json.dumps(first))
        batch_event = await adapter._queue.get()
        assert batch_event.event_type == EventType.DEPTH_DELTA_BATCH
        await adapter.ingest(json.dumps(gap))
        recovery = await adapter._queue.get()
        assert recovery.event_type == EventType.DEPTH_SNAPSHOT
        assert recovery.payload.snapshot_sequence == 200
        assert recovery.exchange_timestamp_ns is None
        assert adapter._last_depth_update == 200
        assert adapter._depth_synced
        assert len(adapter.sequence_gaps) == 1
        assert len(calls) == 2
        await adapter.stop()

    asyncio.run(run())


def test_adapter_lifecycle_can_disconnect_and_reconnect_cleanly():
    class CountingAdapter(BaseAdapter):
        def __init__(self):
            super().__init__("fault-test")
            self.connects = 0
            self.syncs = 0
            self.disconnects = 0
        async def connect(self):
            self.connects += 1
        async def synchronize(self):
            self.syncs += 1
        async def disconnect(self):
            self.disconnects += 1
        async def events(self):
            if False:
                yield None

    async def run():
        adapter = CountingAdapter()
        await adapter.start()
        assert adapter.state == AdapterState.READY
        await adapter.stop()
        assert adapter.state == AdapterState.DISCONNECTED
        await adapter.start()
        assert adapter.state == AdapterState.READY
        await adapter.stop()
        assert (adapter.connects, adapter.syncs, adapter.disconnects) == (2, 2, 2)
        assert adapter.state == AdapterState.DISCONNECTED

    asyncio.run(run())


def test_supervisor_retries_with_bounded_backoff_and_stops_after_recovery():
    async def run():
        attempts = {"count": 0}
        delays = []
        stop = asyncio.Event()

        async def session():
            attempts["count"] += 1
            if attempts["count"] <= 2:
                raise ConnectionError("injected transport disconnect")
            stop.set()

        supervisor = FeedSupervisor(
            BackoffPolicy(max_retries=3, base_seconds=0.001, max_seconds=0.002)
        )
        await supervisor.run(
            session,
            stop_event=stop,
            on_retry=lambda count, delay, error: delays.append((count, delay, str(error))),
        )
        assert attempts["count"] == 3
        assert [item[0] for item in delays] == [1, 2]
        assert [item[1] for item in delays] == [0.001, 0.002]
        assert supervisor.retry_count == 2

    asyncio.run(run())


def test_supervisor_exhausts_retry_budget_without_unbounded_loop():
    async def run():
        attempts = {"count": 0}
        async def session():
            attempts["count"] += 1
            raise ConnectionError("offline")
        supervisor = FeedSupervisor(BackoffPolicy(max_retries=2, base_seconds=0, max_seconds=0))
        with pytest.raises(ConnectionError, match="offline"):
            await supervisor.run(session)
        assert attempts["count"] == 3
        assert supervisor.retry_count == 2

    asyncio.run(run())


def test_instrument_analytics_state_remains_isolated_after_feed_faults():
    registry = AnalyticsRegistry(
        tick_size_by_instrument={
            instrument(profile).canonical_id: (0.1 if profile[2] == "BTCUSDT" else 0.05)
            for profile in PROFILES
        }
    )
    validator = EventValidator()
    manager = DepthRecoveryManager()

    for index, profile in enumerate(PROFILES, start=1):
        event = trade_event(
            profile, seq=1, event_id=f"trade-{profile[2]}",
            exchange_ns=100 + index, receive_ns=200 + index,
        )
        assert validator.validate(event).accepted
        registry.update(event)
        venue, _, _, _, domain = profile
        iid = instrument(profile).canonical_id
        manager.apply_snapshot(venue, iid, domain, snapshot(100))
        result = manager.apply_delta(
            venue, iid, domain, DepthDeltaEvent("BID", 99.0, 1.0),
            sequence_number=102,
        )
        assert not result.accepted and result.health == DepthHealth.DESYNCHRONIZED

    snapshots = registry.snapshots()
    assert len(snapshots) == 4
    assert {item.instrument_id for item in snapshots} == {
        instrument(profile).canonical_id for profile in PROFILES
    }
    assert {profile[3] for profile in PROFILES} == {"INR", "USDT"}
    assert all(manager.health(profile[0], instrument(profile).canonical_id, profile[4])
               == DepthHealth.DESYNCHRONIZED for profile in PROFILES)
