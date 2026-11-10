"""NSE SmartAPI adapter: transport frames -> validated canonical envelopes."""
from __future__ import annotations

import asyncio
import json
import time
from typing import AsyncIterator, Callable

from adapters.base.interface import BaseAdapter, AdapterState
from adapters.base.events import EventEnvelope, EventType
from adapters.nse.smartapi.binary_decoder import decode_packet, SmartAPIDecodeError
from adapters.nse.smartapi.connection import SmartAPIWebSocket
from adapters.nse.smartapi.event_mapper import SmartAPIEventMapper
from adapters.nse.smartapi.feed_health import FeedHealthMonitor, FeedStatus
from adapters.nse.smartapi.subscriptions import Subscription
from pipeline.depth_recovery import DepthRecoveryManager, DepthHealth


class SmartAPIFrameError(ValueError):
    pass


class SmartAPILiveAdapter(BaseAdapter):
    """No aggressor inference, no synthetic trades, no receive-clock substitution.

    JSON frames are control/acknowledgement messages only. Market data must use
    the strict binary decoder. Mode-4 packets are snapshots and have no
    documented sequence number; this adapter never fabricates one.
    """
    _JSON_KEYS = frozenset({
        "status", "errorcode", "message", "task", "channel", "token",
        "exchangeType", "correlationID", "data",
    })

    def __init__(self, connection: SmartAPIWebSocket, mapper: SmartAPIEventMapper,
                 subscriptions: tuple[Subscription, ...], *,
                 health: FeedHealthMonitor | None = None,
                 depth_recovery: DepthRecoveryManager | None = None,
                 receive_clock: Callable[[], int] = time.time_ns,
                 monotonic_clock: Callable[[], int] = time.monotonic_ns,
                 on_depth_resync: Callable[[str, str], object] | None = None,
                 index_tokens: frozenset[str] = frozenset()):
        super().__init__("NSE-SmartAPI")
        required_connection_methods = ("connect", "subscribe", "recv", "disconnect")
        if any(not callable(getattr(connection, name, None)) for name in required_connection_methods):
            raise TypeError("connection must implement connect/subscribe/recv/disconnect")
        if not isinstance(subscriptions, tuple) or not all(isinstance(x, Subscription) for x in subscriptions):
            raise TypeError("subscriptions must be a tuple of Subscription")
        self.connection, self.mapper = connection, mapper
        self.subscriptions = subscriptions
        self.health = health or FeedHealthMonitor()
        self.depth_recovery = depth_recovery or DepthRecoveryManager()
        self.receive_clock, self.monotonic_clock = receive_clock, monotonic_clock
        self.on_depth_resync = on_depth_resync
        if not isinstance(index_tokens, frozenset) or any(not str(x).isdigit() for x in index_tokens):
            raise TypeError("index_tokens must be a frozenset of numeric SmartAPI tokens")
        self._index_tokens = index_tokens
        self._event_counter = 0
        self._last_health = {}

    async def connect(self):
        await self.connection.connect()

    async def synchronize(self):
        # Desired subscriptions are re-applied on every successful reconnect.
        for subscription in self.subscriptions:
            await self.connection.subscribe(subscription)
        if not self.connection.connected:
            raise RuntimeError("SmartAPI connection lost during subscription synchronization")

    async def disconnect(self):
        self.health.mark_disconnected()
        await self.connection.disconnect()

    @classmethod
    def decode_json_control(cls, frame: str):
        try:
            obj = json.loads(frame)
        except json.JSONDecodeError as exc:
            raise SmartAPIFrameError("invalid JSON control frame") from exc
        if not isinstance(obj, dict):
            raise SmartAPIFrameError("JSON control frame must be an object")
        unknown = set(obj) - cls._JSON_KEYS
        if unknown:
            raise SmartAPIFrameError(f"unknown JSON control fields: {sorted(unknown)}")
        if not obj:
            raise SmartAPIFrameError("empty JSON control frame")
        if "status" in obj and not isinstance(obj["status"], bool):
            raise SmartAPIFrameError("status must be boolean")
        if "errorcode" in obj and not isinstance(obj["errorcode"], (str, int)):
            raise SmartAPIFrameError("errorcode must be string or integer")
        if "message" in obj and not isinstance(obj["message"], str):
            raise SmartAPIFrameError("message must be a string")
        if obj.get("status") is False or str(obj.get("errorcode", "0")) not in ("0", "", "None"):
            raise SmartAPIFrameError(f"provider control error: {obj.get('message', 'unspecified')}")
        return obj

    async def _resubscribe_depth_token(self, token: str):
        # A sequenced quote-stream gap invalidates confidence in the associated
        # instrument feed. Request a fresh provider depth snapshot where a
        # matching mode-4 subscription exists. Mode-4 itself carries no
        # documented sequence number, so no missing depth sequence is invented.
        for subscription in self.subscriptions:
            if subscription.mode == 4 and any(
                token in group.tokens for group in subscription.token_groups
            ):
                await self.connection.subscribe(subscription)

    async def _handle_depth_snapshot(self, event: EventEnvelope):
        payload = event.payload
        key = (event.venue, event.instrument.canonical_id, "SMARTAPI:DEPTH")
        result = self.depth_recovery.apply_snapshot(
            event.venue, event.instrument.canonical_id, "SMARTAPI:DEPTH", payload
        )
        if result.accepted:
            self.health.recover_with_snapshot(*key)
        return result

    async def events(self) -> AsyncIterator[EventEnvelope]:
        if self.state != AdapterState.READY:
            raise RuntimeError("adapter must be READY before consuming events")
        while True:
            frame = await self.connection.recv()
            if isinstance(frame, str):
                if frame.strip().lower() == "pong":
                    continue
                self.decode_json_control(frame)
                continue
            if not isinstance(frame, (bytes, bytearray, memoryview)):
                raise SmartAPIFrameError("unsupported WebSocket frame type")
            receive_ns = self.receive_clock()
            if isinstance(receive_ns, bool) or not isinstance(receive_ns, int) or receive_ns < 0:
                raise SmartAPIFrameError("receive clock must return non-negative epoch nanoseconds")
            packet = decode_packet(frame, index_tokens=self._index_tokens)
            self._event_counter += 1
            event = self.mapper.map_packet(
                packet, receive_timestamp_ns=receive_ns,
                event_id=f"smartapi-live-{self._event_counter}",
            )
            health = self.health.observe(event, monotonic_ns=self.monotonic_clock())
            self._last_health[(event.instrument.canonical_id, event.sequence_domain)] = health

            if event.event_type == EventType.DEPTH_SNAPSHOT:
                await self._handle_depth_snapshot(event)
            elif event.sequence_number is not None and health.status == FeedStatus.DESYNCHRONIZED:
                if self.on_depth_resync is not None:
                    result = self.on_depth_resync(event.instrument.canonical_id, event.sequence_domain)
                    if asyncio.iscoroutine(result):
                        await result
                else:
                    await self._resubscribe_depth_token(packet.token)
            yield event

    def health_snapshot(self):
        return dict(self._last_health)
