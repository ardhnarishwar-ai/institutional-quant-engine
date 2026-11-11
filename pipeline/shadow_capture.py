"""Read-only bounded shadow capture for already-constructed live feed adapters.

This layer only persists canonical EventEnvelope objects. It deliberately does
not call analytics, risk, paper-trading, ledger, or execution components.
Adapters remain responsible for their venue-specific transport and parsing.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Iterable, Optional

from adapters.base.events import EventEnvelope
from adapters.base.interface import AdapterState
from pipeline.event_journal import EventJournal


@dataclass(frozen=True, slots=True)
class ShadowFeedFailure:
    adapter_name: str
    error_type: str
    message: str


@dataclass(frozen=True, slots=True)
class ShadowCaptureStats:
    received: int
    persisted: int
    failures: int
    queue_depth: int
    execution_mode: str = "DISABLED"


class ShadowJournalFailure(RuntimeError):
    """Durable capture failed; session must stop accepting new events."""


class ReadOnlyShadowCapture:
    """Bounded multi-adapter event capture with per-feed fault isolation.

    Journal append is the acceptance point. No event is counted as persisted
    before append returns. A journal failure is session-fatal and fail-closed.
    A producer failure is isolated and reported; other feeds continue.
    """

    execution_mode = "DISABLED"

    def __init__(
        self,
        adapters: Iterable,
        *,
        journal: EventJournal,
        queue_maxsize: int = 256,
        shutdown_timeout: float = 5.0,
    ):
        if isinstance(queue_maxsize, bool) or not isinstance(queue_maxsize, int) or queue_maxsize < 1:
            raise ValueError("queue_maxsize must be a positive integer")
        if shutdown_timeout <= 0:
            raise ValueError("shutdown_timeout must be positive")
        self.adapters = tuple(adapters)
        if not self.adapters:
            raise ValueError("at least one adapter is required")
        self.journal = journal
        self.queue_maxsize = queue_maxsize
        self.shutdown_timeout = float(shutdown_timeout)
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=queue_maxsize)
        self._stop = asyncio.Event()
        self._fatal = asyncio.Event()
        self._producers: list[asyncio.Task] = []
        self._writer: Optional[asyncio.Task] = None
        self._started = []
        self._failures: list[ShadowFeedFailure] = []
        self._received = 0
        self._persisted = 0
        self._fatal_error: Optional[BaseException] = None
        self._closed = False

    @property
    def failures(self) -> tuple[ShadowFeedFailure, ...]:
        return tuple(self._failures)

    @property
    def fatal_error(self) -> Optional[BaseException]:
        return self._fatal_error

    @property
    def queue_depth(self) -> int:
        return self._queue.qsize()

    def stats(self) -> ShadowCaptureStats:
        return ShadowCaptureStats(
            received=self._received,
            persisted=self._persisted,
            failures=len(self._failures),
            queue_depth=self.queue_depth,
        )

    def _record_failure(self, adapter_name: str, exc: BaseException) -> None:
        self._failures.append(
            ShadowFeedFailure(str(adapter_name), type(exc).__name__, str(exc))
        )

    async def start(self) -> None:
        if self._writer is not None or self._producers:
            raise RuntimeError("shadow capture already started")
        self._stop.clear()
        self._fatal.clear()
        self._closed = False
        started = []
        try:
            for adapter in self.adapters:
                if adapter.state != AdapterState.READY:
                    await adapter.start()
                started.append(adapter)
            self._started = started
            self._writer = asyncio.create_task(self._write_loop(), name="shadow-journal-writer")
            self._producers = [
                asyncio.create_task(self._capture_feed(adapter), name=f"shadow-{adapter.adapter_name}")
                for adapter in self.adapters
            ]
        except BaseException:
            for adapter in reversed(started):
                try:
                    await adapter.stop()
                except BaseException as cleanup_exc:
                    self._record_failure(adapter.adapter_name, cleanup_exc)
            self._started = []
            self._writer = None
            self._producers = []
            raise

    async def _capture_feed(self, adapter) -> None:
        try:
            async for event in adapter.guarded_events():
                if not isinstance(event, EventEnvelope):
                    raise TypeError("adapter yielded a non-EventEnvelope value")
                if self._stop.is_set() or self._fatal.is_set():
                    break
                await self._queue.put((adapter.adapter_name, event))
                self._received += 1
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            self._record_failure(adapter.adapter_name, exc)

    async def _write_loop(self) -> None:
        while True:
            item = await self._queue.get()
            try:
                if item is None:
                    return
                adapter_name, event = item
                if self._fatal.is_set():
                    continue
                try:
                    self.journal.append(event)
                except BaseException as exc:
                    self._fatal_error = ShadowJournalFailure(
                        f"durable shadow append failed for {event.event_id}"
                    )
                    self._fatal_error.__cause__ = exc
                    self._record_failure("SHADOW_JOURNAL", self._fatal_error)
                    self._fatal.set()
                    self._stop.set()
                    # A producer may be blocked on bounded queue.put(). Cancel
                    # every feed producer immediately: no unjournaled intake
                    # may continue after durable append has failed. stop()
                    # performs the awaited joins and adapter disconnects.
                    for producer in tuple(self._producers):
                        if producer is not asyncio.current_task() and not producer.done():
                            producer.cancel()
                    return
                self._persisted += 1
            finally:
                self._queue.task_done()

    async def run(self) -> ShadowCaptureStats:
        await self.start()
        try:
            results = await asyncio.gather(*self._producers, return_exceptions=True)
            if self._fatal_error is None:
                for result in results:
                    if isinstance(result, BaseException):
                        raise result
                await self._drain_and_close_writer()
        finally:
            await self.stop()
        if self._fatal_error is not None:
            raise self._fatal_error
        return self.stats()

    async def _drain_and_close_writer(self) -> None:
        if self._writer is None:
            return
        await self._queue.join()
        if not self._writer.done():
            await self._queue.put(None)
        await self._writer

    async def stop(self) -> None:
        if self._closed:
            return
        self._stop.set()
        producers, self._producers = self._producers, []
        try:
            if producers:
                try:
                    await asyncio.wait_for(
                        asyncio.gather(*producers, return_exceptions=True),
                        timeout=self.shutdown_timeout,
                    )
                except asyncio.TimeoutError:
                    for task in producers:
                        task.cancel()
                    await asyncio.gather(*producers, return_exceptions=True)

            writer = self._writer
            if writer is not None and not writer.done():
                try:
                    await asyncio.wait_for(self._queue.join(), timeout=self.shutdown_timeout)
                except asyncio.TimeoutError:
                    writer.cancel()
                    await asyncio.gather(writer, return_exceptions=True)
                    while not self._queue.empty():
                        try:
                            self._queue.get_nowait()
                            self._queue.task_done()
                        except asyncio.QueueEmpty:
                            break
                    self._record_failure("SHADOW_SHUTDOWN", TimeoutError("journal drain deadline exceeded"))
                else:
                    await self._queue.put(None)
                    await asyncio.gather(writer, return_exceptions=True)
        except asyncio.CancelledError:
            for task in producers:
                if not task.done():
                    task.cancel()
            if producers:
                await asyncio.gather(*producers, return_exceptions=True)
            if self._writer is not None and not self._writer.done():
                self._writer.cancel()
                await asyncio.gather(self._writer, return_exceptions=True)
            raise
        finally:
            for adapter in reversed(self._started):
                if adapter.state != AdapterState.DISCONNECTED:
                    try:
                        await adapter.stop()
                    except BaseException as exc:
                        self._record_failure(adapter.adapter_name, exc)
            self._started = []
            self._writer = None
            self._closed = True

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.stop()
        return False
