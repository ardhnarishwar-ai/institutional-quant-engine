"""Async SmartAPI WebSocket Streaming 2.0 transport.

This module owns only socket/authentication/heartbeat/subscription transport.
Binary packet decoding and canonical EventEnvelope construction belong to Phase 3C.
"""
from __future__ import annotations

import asyncio
import json
from typing import Any, Callable

from adapters.nse.smartapi.config import SmartAPIConfig
from adapters.nse.smartapi.subscriptions import (
    MAX_SESSION_SUBSCRIPTIONS, Subscription,
)


class SmartAPIConnectionError(ConnectionError):
    pass


class SmartAPIWebSocket:
    def __init__(
        self,
        config: SmartAPIConfig,
        *,
        connector: Callable[..., Any] | None = None,
    ):
        self.config = config
        self._connector = connector
        self._ws = None
        self._heartbeat_task: asyncio.Task | None = None
        self._send_lock = asyncio.Lock()
        self._subscriptions: dict[tuple, Subscription] = {}
        self._heartbeat_error: BaseException | None = None
        self._closing = False

    @property
    def connected(self) -> bool:
        return self._ws is not None

    @property
    def desired_subscriptions(self) -> tuple[Subscription, ...]:
        return tuple(self._subscriptions.values())

    def _get_connector(self):
        if self._connector is not None:
            return self._connector
        try:
            from websockets.asyncio.client import connect
        except ImportError as exc:
            raise RuntimeError(
                "websockets>=14,<16 is required; install requirements.txt"
            ) from exc
        return connect

    async def connect(self) -> None:
        if self.connected:
            return
        connector = self._get_connector()
        last_error = None
        for attempt in range(self.config.max_retries + 1):
            try:
                self._ws = await connector(
                    self.config.websocket_url,
                    additional_headers=self.config.headers(),
                    open_timeout=self.config.open_timeout_seconds,
                    close_timeout=self.config.close_timeout_seconds,
                    ping_interval=None,
                    max_size=2**20,
                )
                self._heartbeat_error = None
                self._closing = False
                self._heartbeat_task = asyncio.create_task(
                    self._heartbeat_loop(), name="smartapi-heartbeat"
                )
                return
            except Exception as exc:
                last_error = exc
                self._ws = None
                if attempt >= self.config.max_retries:
                    break
                delay = min(
                    self.config.retry_base_seconds * (2 ** attempt),
                    self.config.retry_max_seconds,
                )
                if delay:
                    await asyncio.sleep(delay)
        raise SmartAPIConnectionError(
            f"SmartAPI WebSocket connection failed after {self.config.max_retries + 1} attempts"
        ) from last_error

    async def _heartbeat_loop(self) -> None:
        try:
            while self._ws is not None and not self._closing:
                await asyncio.sleep(self.config.heartbeat_interval_seconds)
                if self._ws is None or self._closing:
                    return
                await self._ws.send("ping")
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            self._heartbeat_error = exc
            ws = self._ws
            if ws is not None:
                try:
                    await ws.close()
                except BaseException:
                    pass

    async def send_json(self, payload: dict) -> None:
        if self._ws is None:
            raise SmartAPIConnectionError("WebSocket is not connected")
        if self._heartbeat_error is not None:
            raise SmartAPIConnectionError("heartbeat task failed") from self._heartbeat_error
        encoded = json.dumps(payload, separators=(",", ":"), allow_nan=False)
        async with self._send_lock:
            await self._ws.send(encoded)

    @staticmethod
    def _subscription_key(subscription: Subscription):
        return subscription.mode, tuple(
            (group.exchange_type, group.tokens) for group in subscription.token_groups
        )

    def _total_desired_count(self, candidate: Subscription | None = None) -> int:
        subscriptions = list(self._subscriptions.values())
        if candidate is not None and self._subscription_key(candidate) not in self._subscriptions:
            subscriptions.append(candidate)
        return sum(item.subscription_count for item in subscriptions)

    async def subscribe(self, subscription: Subscription, correlation_id: str | None = None):
        if not isinstance(subscription, Subscription):
            raise TypeError("subscription must be a Subscription")
        key = self._subscription_key(subscription)
        if key not in self._subscriptions and self._total_desired_count(subscription) > MAX_SESSION_SUBSCRIPTIONS:
            raise ValueError(f"session subscription quota is {MAX_SESSION_SUBSCRIPTIONS}")
        await self.send_json(subscription.payload(action=1, correlation_id=correlation_id))
        self._subscriptions[key] = subscription

    async def unsubscribe(self, subscription: Subscription, correlation_id: str | None = None):
        if not isinstance(subscription, Subscription):
            raise TypeError("subscription must be a Subscription")
        await self.send_json(subscription.payload(action=0, correlation_id=correlation_id))
        self._subscriptions.pop(self._subscription_key(subscription), None)

    async def recv(self):
        """Return the next non-heartbeat raw frame (bytes or text)."""
        if self._ws is None:
            raise SmartAPIConnectionError("WebSocket is not connected")
        while True:
            if self._heartbeat_error is not None:
                raise SmartAPIConnectionError("heartbeat task failed") from self._heartbeat_error
            frame = await self._ws.recv()
            if isinstance(frame, str) and frame.strip().lower() == "pong":
                continue
            return frame

    async def reconnect(self) -> None:
        """Reconnect with bounded retries and replay desired subscriptions."""
        desired = tuple(self._subscriptions.values())
        await self.disconnect()
        await self.connect()
        for subscription in desired:
            await self.send_json(subscription.payload(action=1))

    async def disconnect(self) -> None:
        self._closing = True
        task, self._heartbeat_task = self._heartbeat_task, None
        if task is not None and task is not asyncio.current_task():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            except BaseException:
                pass
        ws, self._ws = self._ws, None
        if ws is not None:
            await ws.close()
        self._closing = False

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.disconnect()
        return False
