import asyncio
import json
import struct

import pytest

from adapters.base.interface import AdapterState
from adapters.base.instrument import InstrumentId, InstrumentKind
from adapters.nse.smartapi.adapter import SmartAPILiveAdapter, SmartAPIFrameError
from adapters.nse.smartapi.config import SmartAPIConfig
from adapters.nse.smartapi.connection import SmartAPIWebSocket
from adapters.nse.smartapi.event_mapper import SmartAPIEventMapper
from adapters.nse.smartapi.feed_health import FeedHealthMonitor, FeedStatus
from adapters.nse.smartapi.feed_supervisor import BackoffPolicy, FeedSupervisor
from adapters.nse.smartapi.instrument_master import SmartAPIInstrumentMaster
from adapters.nse.smartapi.subscriptions import ExchangeTokens, Subscription


def master():
    return SmartAPIInstrumentMaster([{
        "exch_seg": "NSE", "token": "99926000", "symbol": "NIFTY",
        "name": "NIFTY", "instrumenttype": "AMXIDX", "lotsize": 1,
    }])


def config(**kw):
    values = dict(jwt_token="jwt", api_key="key", client_code="client",
                  feed_token="feed", max_retries=0, heartbeat_interval_seconds=60)
    values.update(kw)
    return SmartAPIConfig(**values)


def ltp_frame(seq=1, timestamp_ms=1_700_000_000_000, price=2200000):
    # SmartAPI mode-1 layout: 43-byte common header + 8-byte LTP.
    token = b"99926000".ljust(25, b"\0")
    return (struct.pack("<BB", 1, 1) + token +
            struct.pack("<qqq", seq, timestamp_ms, price))


class FakeSocket:
    def __init__(self, frames=()):
        self.frames = list(frames)
        self.sent = []
        self.closed = False
    async def send(self, value):
        self.sent.append(value)
    async def recv(self):
        if not self.frames:
            raise ConnectionError("fixture stream ended")
        return self.frames.pop(0)
    async def close(self):
        self.closed = True


class FakeConnection:
    def __init__(self, frames=()):
        self.socket = FakeSocket(frames)
        self.connected = False
        self.subscribed = []
        self.disconnect_calls = 0
    async def connect(self):
        self.connected = True
    async def subscribe(self, subscription):
        self.subscribed.append(subscription)
    async def recv(self):
        return await self.socket.recv()
    async def disconnect(self):
        self.disconnect_calls += 1
        self.connected = False
        await self.socket.close()


def adapter_for(connection, *, frames=(), recv_ns=1_700_000_000_100, mono_ns=500):
    subscription = Subscription(1, (ExchangeTokens(1, ("99926000",)),))
    return SmartAPILiveAdapter(
        connection, SmartAPIEventMapper(master()), (subscription,),
        receive_clock=lambda: recv_ns, monotonic_clock=lambda: mono_ns,
        health=FeedHealthMonitor(stale_after_ns=1000),
    )


def test_bounded_backoff_caps_and_validates():
    policy = BackoffPolicy(max_retries=5, base_seconds=0.5, max_seconds=2)
    assert [policy.delay(i) for i in range(5)] == [0.5, 1.0, 2, 2, 2]
    with pytest.raises(ValueError):
        BackoffPolicy(max_retries=-1)


def test_feed_health_detects_gap_and_preserves_both_clocks():
    from adapters.base.events import EventEnvelope, EventType, QuoteEvent
    inst = InstrumentId("NSE", InstrumentKind.NSE_SPOT, "NIFTY", "INR")
    monitor = FeedHealthMonitor(stale_after_ns=10)
    def event(seq, exch, recv):
        return EventEnvelope("NSE", inst, EventType.QUOTE, exch, recv, seq,
                             "NSE:QUOTE:NIFTY", QuoteEvent(last_price=100), f"e{seq}")
    first = monitor.observe(event(10, 100, 150), monotonic_ns=1)
    assert first.status == FeedStatus.HEALTHY and first.latency_ns == 50
    gap = monitor.observe(event(13, 120, 180), monotonic_ns=2)
    assert gap.status == FeedStatus.DESYNCHRONIZED
    assert gap.sequence_gap == (11, 12)
    stale = monitor.snapshot("NSE", inst.canonical_id, "NSE:QUOTE:NIFTY", monotonic_ns=20)
    assert stale.status == FeedStatus.STALE
    assert stale.last_exchange_timestamp_ns == 120
    assert stale.last_receive_timestamp_ns == 180


def test_negative_measured_latency_is_flagged_not_clamped():
    from adapters.base.events import EventEnvelope, EventType, QuoteEvent
    inst = InstrumentId("NSE", InstrumentKind.NSE_SPOT, "NIFTY", "INR")
    e = EventEnvelope("NSE", inst, EventType.QUOTE, 200, 100, 1, "d",
                      QuoteEvent(last_price=100), "clock-skew")
    result = FeedHealthMonitor().observe(e, monotonic_ns=1)
    assert result.latency_ns == -100
    assert result.clock_skew
    assert "NEGATIVE_MEASURED_LATENCY_CLOCK_SKEW" in result.flags


def test_json_control_schema_accepts_ack_and_rejects_unknown_fields():
    assert SmartAPILiveAdapter.decode_json_control(
        json.dumps({"status": True, "errorcode": "0", "message": "ok"})
    )["status"] is True
    with pytest.raises(SmartAPIFrameError, match="unknown JSON"):
        SmartAPILiveAdapter.decode_json_control('{"status":true,"unexpected":1}')
    with pytest.raises(SmartAPIFrameError, match="provider control error"):
        SmartAPILiveAdapter.decode_json_control(
            '{"status":false,"errorcode":"E1","message":"denied"}'
        )


def test_adapter_maps_binary_fixture_without_inventing_trade():
    async def run():
        connection = FakeConnection([ltp_frame()])
        adapter = adapter_for(connection)
        await adapter.start()
        assert adapter.state == AdapterState.READY
        assert len(connection.subscribed) == 1
        stream = adapter.events()
        event = await anext(stream)
        assert event.event_type.value == "QUOTE"
        assert event.payload.last_price == 22000.0
        assert event.exchange_timestamp_ns == 1_700_000_000_000_000_000
        assert event.receive_timestamp_ns == 1_700_000_000_100
        assert event.event_type.value != "TRADE"
        await adapter.stop()
    asyncio.run(run())


def test_sequence_gap_requests_fresh_depth_snapshot_when_configured():
    async def run():
        depth = Subscription(4, (ExchangeTokens(1, ("99926000",)),))
        connection = FakeConnection([ltp_frame(seq=1), ltp_frame(seq=3)])
        adapter = SmartAPILiveAdapter(
            connection, SmartAPIEventMapper(master()), (depth,),
            receive_clock=lambda: 1_700_000_000_000_000_100,
            monotonic_clock=lambda: 10,
        )
        await adapter.start()
        stream = adapter.events()
        await anext(stream)
        await anext(stream)
        # One subscription at startup, one explicit snapshot refresh on gap.
        assert len(connection.subscribed) == 2
        await adapter.stop()
    asyncio.run(run())


def test_adapter_rejects_malformed_binary_frame():
    async def run():
        connection = FakeConnection([b"\x01\x01"])
        adapter = adapter_for(connection)
        await adapter.start()
        with pytest.raises(ValueError):
            await anext(adapter.events())
        await adapter.stop()
    asyncio.run(run())


def test_adapter_restores_subscriptions_during_each_start():
    async def run():
        connection = FakeConnection([])
        adapter = adapter_for(connection)
        await adapter.start()
        await adapter.stop()
        await adapter.start()
        assert len(connection.subscribed) == 2
        await adapter.stop()
    asyncio.run(run())


def test_supervisor_retries_then_stops_after_success():
    async def run():
        retries = []
        attempts = {"n": 0}
        stop = asyncio.Event()
        async def sleep(delay):
            raise AssertionError("stop_event path should use interruptible event wait")
        async def session():
            attempts["n"] += 1
            if attempts["n"] < 3:
                raise ConnectionError("drop")
            stop.set()
        supervisor = FeedSupervisor(BackoffPolicy(4, 0.25, 1), sleep_fn=sleep)
        await supervisor.run(
            session, stop_event=stop,
            on_retry=lambda count, delay, error: retries.append((count, delay, str(error))),
        )
        assert attempts["n"] == 3
        assert [item[1] for item in retries] == [0.25, 0.5]
    asyncio.run(run())


def test_supervisor_exhausts_retry_budget():
    async def run():
        async def session():
            raise ConnectionError("offline")
        supervisor = FeedSupervisor(BackoffPolicy(2, 0, 0))
        with pytest.raises(ConnectionError, match="offline"):
            await supervisor.run(session)
        assert supervisor.retry_count == 2
    asyncio.run(run())


def test_depth_snapshot_recovery_does_not_invent_sequence():
    from adapters.base.events import DepthSnapshotEvent, DepthSnapshotLevel
    from adapters.base.events import EventEnvelope, EventType
    from pipeline.depth_recovery import DepthRecoveryManager, DepthHealth
    inst = InstrumentId("NSE", InstrumentKind.NSE_SPOT, "NIFTY", "INR")
    snap = DepthSnapshotEvent(
        bids=(DepthSnapshotLevel(99.0, 10),),
        asks=(DepthSnapshotLevel(101.0, 11),), snapshot_sequence=None,
    )
    manager = DepthRecoveryManager()
    result = manager.apply_snapshot("NSE", inst.canonical_id, "SMARTAPI:DEPTH", snap)
    assert result.accepted
    assert manager.book("NSE", inst.canonical_id, "SMARTAPI:DEPTH")["last_sequence"] is None


def test_connection_config_keeps_secure_transport_and_bounded_retry_values():
    cfg = config()
    assert cfg.websocket_url.startswith("wss://")
    assert cfg.max_retries == 0
    with pytest.raises(ValueError):
        config(websocket_url="ws://insecure.example")
