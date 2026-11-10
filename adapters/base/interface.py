"""Abstract adapter contract with guarded lifecycle and guaranteed teardown."""
from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from enum import Enum
from threading import RLock
from typing import AsyncIterator

from adapters.base.events import EventEnvelope


class AdapterState(str, Enum):
    DISCONNECTED = "DISCONNECTED"
    CONNECTING = "CONNECTING"
    SYNCING = "SYNCING"
    READY = "READY"
    STALE = "STALE"


_ALLOWED = {
    AdapterState.DISCONNECTED: {AdapterState.CONNECTING},
    AdapterState.CONNECTING: {AdapterState.SYNCING, AdapterState.STALE, AdapterState.DISCONNECTED},
    AdapterState.SYNCING: {AdapterState.READY, AdapterState.STALE, AdapterState.DISCONNECTED},
    AdapterState.READY: {AdapterState.STALE, AdapterState.DISCONNECTED},
    AdapterState.STALE: {AdapterState.SYNCING, AdapterState.DISCONNECTED},
}


class InvalidStateTransition(RuntimeError):
    pass


class BaseAdapter(ABC):
    """Adapters own transport and venue parsing, never core calculations."""

    def __init__(self, adapter_name: str):
        if not adapter_name or not adapter_name.strip():
            raise ValueError("adapter_name is required")
        self.adapter_name = adapter_name
        self._state = AdapterState.DISCONNECTED
        self._state_lock = RLock()

    @property
    def state(self) -> AdapterState:
        with self._state_lock:
            return self._state

    def transition(self, target: AdapterState):
        if not isinstance(target, AdapterState):
            raise TypeError("target must be AdapterState")
        with self._state_lock:
            if target not in _ALLOWED[self._state]:
                raise InvalidStateTransition(f"{self._state.value} -> {target.value} is not allowed")
            self._state = target

    async def _disconnect_shielded(self, *, suppress_errors: bool = False) -> None:
        """Finish teardown despite cancellation; optionally propagate close errors."""
        cleanup = asyncio.create_task(self.disconnect())
        while not cleanup.done():
            try:
                await asyncio.shield(cleanup)
            except asyncio.CancelledError:
                # Keep waiting for the independent cleanup task. The caller's
                # original exception/cancellation is re-raised by the caller.
                continue
            except BaseException:
                break
        try:
            cleanup.result()
        except BaseException:
            if not suppress_errors:
                raise

    async def start(self):
        """Connect, synchronize, and become READY; rollback every partial start."""
        if self.state != AdapterState.DISCONNECTED:
            raise InvalidStateTransition("start requires DISCONNECTED state")

        connected = False
        self.transition(AdapterState.CONNECTING)
        try:
            await self.connect()
            connected = True
            self.transition(AdapterState.SYNCING)
            await self.synchronize()
            self.transition(AdapterState.READY)
        except BaseException:
            try:
                if connected:
                    await self._disconnect_shielded(suppress_errors=True)
            finally:
                # CONNECTING/SYNCING can always return to DISCONNECTED.
                if self.state != AdapterState.DISCONNECTED:
                    self.transition(AdapterState.DISCONNECTED)
            raise

    async def mark_stale(self):
        if self.state not in (AdapterState.CONNECTING, AdapterState.SYNCING, AdapterState.READY):
            raise InvalidStateTransition("only active adapters can become STALE")
        self.transition(AdapterState.STALE)

    async def stop(self):
        """Close resources and guarantee a terminal DISCONNECTED state."""
        if self.state == AdapterState.DISCONNECTED:
            return
        try:
            await self._disconnect_shielded()
        finally:
            if self.state != AdapterState.DISCONNECTED:
                self.transition(AdapterState.DISCONNECTED)

    async def guarded_events(self) -> AsyncIterator[EventEnvelope]:
        """Forward events and tear down transport if iteration fails unexpectedly."""
        if self.state != AdapterState.READY:
            raise InvalidStateTransition("guarded_events requires READY state")
        try:
            async for event in self.events():
                yield event
        except BaseException:
            await self.stop()
            raise

    @abstractmethod
    async def connect(self) -> None:
        """Open the venue transport."""

    @abstractmethod
    async def synchronize(self) -> None:
        """Acquire snapshots and establish a valid sequence boundary."""

    @abstractmethod
    async def disconnect(self) -> None:
        """Close transport and release resources."""

    @abstractmethod
    def events(self) -> AsyncIterator[EventEnvelope]:
        """Yield normalized events while READY; transport-specific implementation."""
