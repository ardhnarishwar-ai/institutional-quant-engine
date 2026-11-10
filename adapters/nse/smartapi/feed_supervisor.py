"""Bounded reconnect supervisor; desired subscriptions are restored by adapter sync."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Awaitable, Callable, Optional


@dataclass(frozen=True, slots=True)
class BackoffPolicy:
    max_retries: int = 5
    base_seconds: float = 0.5
    max_seconds: float = 8.0

    def __post_init__(self):
        if isinstance(self.max_retries, bool) or not isinstance(self.max_retries, int) or self.max_retries < 0:
            raise ValueError("max_retries must be a non-negative integer")
        if self.base_seconds < 0 or self.max_seconds < self.base_seconds:
            raise ValueError("invalid backoff bounds")

    def delay(self, retry_index: int) -> float:
        if retry_index < 0:
            raise ValueError("retry_index cannot be negative")
        return min(self.base_seconds * (2 ** retry_index), self.max_seconds)


class FeedSupervisor:
    """Runs one adapter session at a time and retries with bounded backoff.

    run_session is expected to return only on an orderly stop; unexpected
    completion is treated as a disconnect. sleep_fn is injectable for tests.
    """
    def __init__(self, policy: BackoffPolicy = BackoffPolicy(), *, sleep_fn=asyncio.sleep):
        self.policy = policy
        self.sleep_fn = sleep_fn
        self.retry_count = 0

    async def run(self, run_session: Callable[[], Awaitable[None]], *,
                  stop_event: Optional[asyncio.Event] = None,
                  on_retry: Optional[Callable[[int, float, BaseException], None]] = None):
        self.retry_count = 0
        while stop_event is None or not stop_event.is_set():
            try:
                await run_session()
                if stop_event is None or not stop_event.is_set():
                    raise ConnectionError("feed session ended unexpectedly")
                return
            except asyncio.CancelledError:
                raise
            except BaseException as exc:
                if self.retry_count >= self.policy.max_retries:
                    raise
                delay = self.policy.delay(self.retry_count)
                self.retry_count += 1
                if on_retry is not None:
                    on_retry(self.retry_count, delay, exc)
                if stop_event is None:
                    await self.sleep_fn(delay)
                else:
                    try:
                        await asyncio.wait_for(stop_event.wait(), timeout=delay)
                    except asyncio.TimeoutError:
                        pass
                    if stop_event.is_set():
                        return
