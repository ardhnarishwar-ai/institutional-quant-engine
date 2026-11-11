"""Concurrent live feed orchestration with bounded intake and fail-closed journaling."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Optional

from adapters.base.events import EventType, DepthDeltaEvent
from adapters.base.depth_batch import DepthDeltaBatchEvent
from adapters.base.interface import AdapterState
from pipeline.event_validator import EventValidator, EventValidationError
from pipeline.event_journal import EventJournal
from pipeline.depth_recovery import DepthRecoveryManager
from pipeline.snapshot_sync import SnapshotSyncCoordinator
from core.microstructure.analytics_state import AnalyticsRegistry


@dataclass(frozen=True, slots=True)
class IngestStats:
    received: int
    accepted: int
    rejected: int
    quarantined: int
    errors: int


@dataclass(frozen=True, slots=True)
class FeedFailure:
    adapter_name: str
    error_type: str
    message: str


class JournalWriteFailure(RuntimeError):
    """Raised when an event or required audit record cannot be persisted."""


class LiveIngestPipeline:
    """Bounded multi-feed ingestion.

    Producers are isolated per adapter. A single ordered processor validates,
    journals, updates depth state and analytics. A journal failure is terminal:
    no event after the failed append is applied to analytics. Shutdown stops
    intake, drains queued work up to ``shutdown_timeout``, then tears adapters
    down. ``execution_mode`` is informational and permanently DISABLED.
    """

    execution_mode = "DISABLED"

    def __init__(
        self, adapters, *, journal: EventJournal, validator: EventValidator,
        analytics: AnalyticsRegistry, depth_recovery=None, snapshot_sync=None,
        reference_levels=None, queue_maxsize: int = 256,
        shutdown_timeout: float = 5.0,
    ):
        if not isinstance(queue_maxsize, int) or isinstance(queue_maxsize, bool) or queue_maxsize < 1:
            raise ValueError("queue_maxsize must be a positive integer")
        if shutdown_timeout <= 0:
            raise ValueError("shutdown_timeout must be positive")
        self.adapters = tuple(adapters)
        self.journal = journal
        self.validator = validator
        self.analytics = analytics
        self.depth_recovery = depth_recovery or DepthRecoveryManager(journal)
        self.snapshot_sync = snapshot_sync or SnapshotSyncCoordinator(self.depth_recovery, journal)
        self.reference_levels = reference_levels or {}
        self.queue_maxsize = queue_maxsize
        self.shutdown_timeout = float(shutdown_timeout)
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=queue_maxsize)
        self._stop = asyncio.Event()
        self._fatal = asyncio.Event()
        self._producers: list[asyncio.Task] = []
        self._processor: Optional[asyncio.Task] = None
        self._started_adapters = []
        self._counts = [0, 0, 0, 0, 0]
        self._failures: list[FeedFailure] = []
        self._fatal_error: Optional[BaseException] = None
        self._closed = False

    def _failure(self, adapter_name, exc):
        self._counts[4] += 1
        self._failures.append(FeedFailure(str(adapter_name), type(exc).__name__, str(exc)))

    @property
    def failures(self):
        return tuple(self._failures)

    @property
    def fatal_error(self):
        return self._fatal_error

    @property
    def queue_depth(self):
        return self._queue.qsize()

    async def _produce(self, adapter):
        try:
            async for event in adapter.guarded_events():
                if self._stop.is_set() or self._fatal.is_set():
                    break
                while not self._fatal.is_set() and not self._stop.is_set():
                    try:
                        await asyncio.wait_for(
                            self._queue.put((adapter, event)), timeout=0.05
                        )
                        self._counts[0] += 1
                        break
                    except asyncio.TimeoutError:
                        continue
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            self._failure(adapter.adapter_name, exc)

    async def _process_one(self, event):
        try:
            result = self.validator.validate(event)
        except EventValidationError as exc:
            try:
                self.journal.append_metadata({
                    "kind": "EVENT_REJECTED", "event_id": event.event_id,
                    "venue": event.venue, "reason": str(exc),
                    "exchange_timestamp_ns": event.exchange_timestamp_ns,
                    "receive_timestamp_ns": event.receive_timestamp_ns,
                })
            except BaseException as journal_exc:
                raise JournalWriteFailure("failed to journal rejected event") from journal_exc
            self._counts[2] += 1
            return

        if not result.accepted:
            try:
                self.journal.append_metadata({
                    "kind": "EVENT_QUARANTINED", "event_id": event.event_id,
                    "venue": event.venue, "sequence_domain": event.sequence_domain,
                    "reason": result.metadata,
                })
            except BaseException as journal_exc:
                raise JournalWriteFailure("failed to journal quarantined event") from journal_exc
            self._counts[2] += 1
            if result.quarantined:
                self._counts[3] += 1
            return

        # Persistence is the commit point: analytics is never updated first.
        try:
            self.journal.append(event)
        except BaseException as exc:
            raise JournalWriteFailure(f"event journal append failed for {event.event_id}") from exc

        if event.event_type == EventType.DEPTH_SNAPSHOT:
            self.snapshot_sync.apply(event)
        elif event.event_type == EventType.DEPTH_DELTA and isinstance(event.payload, DepthDeltaEvent):
            self.depth_recovery.apply_delta(
                event.venue, event.instrument.canonical_id, event.sequence_domain,
                event.payload, sequence_number=event.sequence_number,
            )
        elif event.event_type == EventType.DEPTH_DELTA_BATCH and isinstance(event.payload, DepthDeltaBatchEvent):
            self.depth_recovery.apply_batch(
                event.venue, event.instrument.canonical_id, event.sequence_domain, event.payload,
            )
        self.analytics.update(
            event,
            reference_level=self.reference_levels.get((event.venue, event.instrument.canonical_id)),
        )
        self._counts[1] += 1

    async def _process(self):
        while True:
            item = await self._queue.get()
            try:
                if item is None:
                    return
                _, event = item
                if self._fatal.is_set():
                    continue
                await self._process_one(event)
            except JournalWriteFailure as exc:
                self._fatal_error = exc
                self._failure("JOURNAL", exc)
                self._fatal.set()
                self._stop.set()
                return
            except asyncio.CancelledError:
                raise
            except BaseException as exc:
                # A downstream processing exception is visible and terminal:
                # advancing later events could violate per-stream ordering.
                self._fatal_error = exc
                self._failure("PIPELINE_PROCESSOR", exc)
                self._fatal.set()
                self._stop.set()
                return
            finally:
                self._queue.task_done()

    async def start(self):
        if self._processor is not None or self._producers:
            raise RuntimeError("pipeline already started")
        self._stop.clear()
        self._fatal.clear()
        self._closed = False
        started = []
        try:
            for adapter in self.adapters:
                if adapter.state != AdapterState.READY:
                    await adapter.start()
                started.append(adapter)
            self._started_adapters = started
            self._processor = asyncio.create_task(self._process(), name="live-ingest-processor")
            self._producers = [
                asyncio.create_task(self._produce(adapter), name=f"live-ingest-{adapter.adapter_name}")
                for adapter in self.adapters
            ]
        except BaseException:
            for adapter in reversed(started):
                try:
                    await adapter.stop()
                except BaseException as cleanup_exc:
                    self._failure(adapter.adapter_name, cleanup_exc)
            self._started_adapters = []
            self._processor = None
            self._producers = []
            raise

    async def run(self):
        await self.start()
        try:
            await asyncio.gather(*self._producers)
            await self._finish_processor()
        finally:
            await self.stop()
        if self._fatal_error is not None:
            raise self._fatal_error

    async def _finish_processor(self):
        if self._processor is None:
            return
        if not self._processor.done():
            await self._queue.put(None)
        await self._processor

    async def stop(self):
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

            processor = self._processor
            if processor is not None and not processor.done():
                try:
                    await asyncio.wait_for(self._queue.join(), timeout=self.shutdown_timeout)
                except asyncio.TimeoutError:
                    # Do not pretend the queue drained. Cancel processor and
                    # account for any unprocessed items as errors.
                    processor.cancel()
                    await asyncio.gather(processor, return_exceptions=True)
                    while not self._queue.empty():
                        try:
                            self._queue.get_nowait()
                            self._queue.task_done()
                        except asyncio.QueueEmpty:
                            break
                    self._failure("PIPELINE_SHUTDOWN", TimeoutError("drain deadline exceeded"))
                else:
                    await self._queue.put(None)
                    await asyncio.gather(processor, return_exceptions=True)
            elif processor is not None:
                await asyncio.gather(processor, return_exceptions=True)
        except asyncio.CancelledError:
            for task in producers:
                if not task.done():
                    task.cancel()
            if producers:
                await asyncio.gather(*producers, return_exceptions=True)
            if self._processor is not None and not self._processor.done():
                self._processor.cancel()
                await asyncio.gather(self._processor, return_exceptions=True)
            raise
        finally:
            for adapter in reversed(self._started_adapters or list(self.adapters)):
                if adapter.state != AdapterState.DISCONNECTED:
                    try:
                        await adapter.stop()
                    except BaseException as exc:
                        self._failure(adapter.adapter_name, exc)
            self._started_adapters = []
            self._processor = None
            self._closed = True

    def stats(self):
        return IngestStats(*self._counts)
