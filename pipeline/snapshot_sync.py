"""Snapshot boundary coordination without fabricating provider sequence IDs."""
from dataclasses import dataclass
from adapters.base.events import EventEnvelope,EventType,DepthSnapshotEvent
from pipeline.depth_recovery import DepthRecoveryManager

@dataclass(frozen=True,slots=True)
class SnapshotSyncStatus:
    venue:str
    instrument_id:str
    sequence_domain:str|None
    snapshot_sequence:int|None
    sequence_tracked:bool
    recovered:bool
    book_version:int

class SnapshotSyncCoordinator:
    def __init__(self,depth_manager:DepthRecoveryManager,journal=None):
        self.depth_manager=depth_manager; self.journal=journal; self._status={}
    def apply(self,event:EventEnvelope):
        if event.event_type!=EventType.DEPTH_SNAPSHOT or not isinstance(event.payload,DepthSnapshotEvent):
            raise TypeError('DEPTH_SNAPSHOT event required')
        domain=event.sequence_domain or f'{event.venue}:UNSEQUENCED_DEPTH'
        result=self.depth_manager.apply_snapshot(event.venue,event.instrument.canonical_id,domain,event.payload)
        status=SnapshotSyncStatus(event.venue,event.instrument.canonical_id,event.sequence_domain,
            event.payload.snapshot_sequence,event.payload.snapshot_sequence is not None,result.recovered,result.book_version)
        self._status[(event.venue,event.instrument.canonical_id,event.sequence_domain)]=status
        if self.journal:
            self.journal.append_metadata({'kind':'SNAPSHOT_SYNC','venue':event.venue,
                'instrument_id':event.instrument.canonical_id,'sequence_domain':event.sequence_domain,
                'snapshot_sequence':event.payload.snapshot_sequence,
                'sequence_tracking':event.payload.snapshot_sequence is not None,
                'recovered':result.recovered,'book_version':result.book_version,
                'exchange_timestamp_ns':event.exchange_timestamp_ns,
                'receive_timestamp_ns':event.receive_timestamp_ns})
        return status
    def status(self,venue,instrument_id,sequence_domain=None):
        return self._status.get((venue.upper(),instrument_id,sequence_domain))
