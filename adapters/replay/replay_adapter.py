"""Deterministic replay adapter with realtime, accelerated and step modes."""
from __future__ import annotations

import asyncio
from typing import AsyncIterator, Iterable

from adapters.base.events import EventEnvelope
from adapters.base.interface import AdapterState, BaseAdapter
from adapters.replay.clock import ReplayClock
from adapters.replay.ordering import merge_event_streams


class ReplayAdapter(BaseAdapter):
    def __init__(self, event_streams, speed=1.0, step_mode=False, validator=None):
        super().__init__("historical-replay")
        self._source_streams = tuple(event_streams)
        self.clock = ReplayClock(speed)
        self.step_mode = step_mode
        self.validator = validator
        self._events = None
        self._step_queue = asyncio.Queue()
        self.quarantined = []

    async def connect(self):
        self._events = iter(merge_event_streams(self._source_streams))

    async def synchronize(self):
        if self._events is None:
            raise RuntimeError("replay source not connected")

    async def disconnect(self):
        self.clock.reset()
        self._events = None

    async def step(self):
        """Release exactly one event in step mode; raises if not in step mode."""
        if not self.step_mode:
            raise RuntimeError("step() requires step_mode=True")
        await self._step_queue.put(True)

    async def events(self) -> AsyncIterator[EventEnvelope]:
        if self.state != AdapterState.READY:
            raise RuntimeError("replay adapter must be READY before consuming events")
        assert self._events is not None
        for event in self._events:
            if self.step_mode:
                await self._step_queue.get()
            else:
                await self.clock.wait_until(event.exchange_timestamp_ns)
            if self.validator is not None:
                result = self.validator.validate(event)
                if not result.accepted:
                    self.quarantined.append((event.event_id, result))
                    continue
            yield event
