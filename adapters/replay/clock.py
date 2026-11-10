"""Replay clock; source timestamps remain untouched."""
from __future__ import annotations

import asyncio
from time import monotonic_ns


class ReplayClock:
    def __init__(self, speed=1.0):
        self.set_speed(speed)
        self._wall_anchor_ns = None
        self._event_anchor_ns = None
        self._last_event_ns = None

    def set_speed(self, speed):
        if speed not in ("max", "unthrottled") and (not isinstance(speed, (int, float)) or speed <= 0):
            raise ValueError("speed must be positive numeric, 'max' or 'unthrottled'")
        self.speed = speed

    def reset(self):
        self._wall_anchor_ns = None
        self._event_anchor_ns = None
        self._last_event_ns = None

    async def wait_until(self, exchange_timestamp_ns):
        if self.speed in ("max", "unthrottled"):
            self._last_event_ns = exchange_timestamp_ns
            return
        if self._wall_anchor_ns is None:
            self._wall_anchor_ns = monotonic_ns()
            self._event_anchor_ns = exchange_timestamp_ns
        target_wall = self._wall_anchor_ns + int(
            (exchange_timestamp_ns - self._event_anchor_ns) / float(self.speed))
        delay_ns = target_wall - monotonic_ns()
        if delay_ns > 0:
            await asyncio.sleep(delay_ns / 1_000_000_000)
        self._last_event_ns = exchange_timestamp_ns
