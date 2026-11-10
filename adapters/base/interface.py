"""Abstract adapter contract with explicit, guarded lifecycle transitions."""
from __future__ import annotations

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

    async def start(self):
        if self.state != AdapterState.DISCONNECTED:
            raise InvalidStateTransition("start requires DISCONNECTED state")
        self.transition(AdapterState.CONNECTING)
        try:
            await self.connect()
            self.transition(AdapterState.SYNCING)
            await self.synchronize()
            self.transition(AdapterState.READY)
        except Exception:
            if self.state != AdapterState.DISCONNECTED:
                self.transition(AdapterState.DISCONNECTED)
            raise

    async def mark_stale(self):
        if self.state not in (AdapterState.CONNECTING, AdapterState.SYNCING, AdapterState.READY):
            raise InvalidStateTransition("only active adapters can become STALE")
        self.transition(AdapterState.STALE)

    async def stop(self):
        if self.state == AdapterState.DISCONNECTED:
            return
        await self.disconnect()
        self.transition(AdapterState.DISCONNECTED)

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
