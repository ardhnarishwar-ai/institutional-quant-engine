"""Concurrent live feed orchestration: validate, journal, book, analytics."""
import asyncio
from dataclasses import dataclass
from adapters.base.events import EventType,DepthDeltaEvent
from adapters.base.depth_batch import DepthDeltaBatchEvent
from adapters.base.interface import AdapterState
from pipeline.event_validator import EventValidator,EventValidationError
from pipeline.event_journal import EventJournal
from pipeline.depth_recovery import DepthRecoveryManager
from pipeline.snapshot_sync import SnapshotSyncCoordinator
from core.microstructure.analytics_state import AnalyticsRegistry

@dataclass(frozen=True,slots=True)
class IngestStats:
    received:int; accepted:int; rejected:int; quarantined:int; errors:int

class LiveIngestPipeline:
    def __init__(self,adapters,*,journal:EventJournal,validator:EventValidator,
                 analytics:AnalyticsRegistry,depth_recovery=None,snapshot_sync=None,reference_levels=None):
        self.adapters=tuple(adapters); self.journal=journal; self.validator=validator; self.analytics=analytics
        self.depth_recovery=depth_recovery or DepthRecoveryManager(journal)
        self.snapshot_sync=snapshot_sync or SnapshotSyncCoordinator(self.depth_recovery,journal)
        self.reference_levels=reference_levels or {}; self._stop=asyncio.Event(); self._tasks=[]
        self._counts=[0,0,0,0,0]
    async def _consume(self,adapter):
        async for event in adapter.events():
            if self._stop.is_set(): break
            self._counts[0]+=1
            try: result=self.validator.validate(event)
            except EventValidationError as exc:
                self._counts[2]+=1
                self.journal.append_metadata({'kind':'EVENT_REJECTED','event_id':event.event_id,
                    'venue':event.venue,'reason':str(exc),'exchange_timestamp_ns':event.exchange_timestamp_ns,
                    'receive_timestamp_ns':event.receive_timestamp_ns})
                continue
            if not result.accepted:
                self._counts[2]+=1
                if result.quarantined:self._counts[3]+=1
                self.journal.append_metadata({'kind':'EVENT_QUARANTINED','event_id':event.event_id,
                    'venue':event.venue,'sequence_domain':event.sequence_domain,'reason':result.metadata})
                continue
            self.journal.append(event)
            if event.event_type==EventType.DEPTH_SNAPSHOT:self.snapshot_sync.apply(event)
            elif event.event_type==EventType.DEPTH_DELTA and isinstance(event.payload,DepthDeltaEvent):
                self.depth_recovery.apply_delta(event.venue,event.instrument.canonical_id,event.sequence_domain,
                    event.payload,sequence_number=event.sequence_number)
            elif event.event_type==EventType.DEPTH_DELTA_BATCH and isinstance(event.payload,DepthDeltaBatchEvent):
                self.depth_recovery.apply_batch(event.venue,event.instrument.canonical_id,event.sequence_domain,event.payload)
            self.analytics.update(event,reference_level=self.reference_levels.get((event.venue,event.instrument.canonical_id)))
            self._counts[1]+=1
    async def start(self):
        if self._tasks:raise RuntimeError('pipeline already started')
        self._stop.clear(); started=[]
        try:
            for adapter in self.adapters:
                if adapter.state!=AdapterState.READY: await adapter.start()
                started.append(adapter)
            self._tasks=[asyncio.create_task(self._consume(a),name=f'live-ingest-{a.name}') for a in self.adapters]
        except BaseException:
            for adapter in reversed(started):
                try:await adapter.stop()
                except BaseException:pass
            raise
    async def run(self):
        await self.start()
        try:await asyncio.gather(*self._tasks)
        finally:await self.stop()
    async def stop(self):
        self._stop.set(); tasks,self._tasks=self._tasks,[]
        for task in tasks:task.cancel()
        if tasks:await asyncio.gather(*tasks,return_exceptions=True)
        for adapter in reversed(self.adapters):
            if adapter.state!=AdapterState.DISCONNECTED:
                try:await adapter.stop()
                except BaseException:self._counts[4]+=1
    def stats(self):return IngestStats(*self._counts)
