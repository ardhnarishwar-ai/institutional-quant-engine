import asyncio
import json
from adapters.base.events import EventEnvelope,EventType,TradeEvent,DepthSnapshotEvent,DepthSnapshotLevel,DepthDeltaChange
from adapters.base.depth_batch import DepthDeltaBatchEvent
from adapters.base.instrument import InstrumentId,InstrumentKind
from adapters.base.interface import BaseAdapter,AdapterState
from core.microstructure.analytics_state import AnalyticsRegistry
from pipeline.depth_recovery import DepthRecoveryManager,DepthHealth
from pipeline.event_journal import EventJournal
from pipeline.event_validator import EventValidator
from pipeline.live_ingest import LiveIngestPipeline
from pipeline.snapshot_sync import SnapshotSyncCoordinator

class FakeAdapter(BaseAdapter):
    def __init__(self,name,items):
        super().__init__(name);self.name=name;self.items=list(items);self.disconnected=False
    async def connect(self):pass
    async def synchronize(self):pass
    async def disconnect(self):self.disconnected=True
    async def events(self):
        if self.state!=AdapterState.READY:raise RuntimeError("adapter not READY")
        for item in self.items:
            if self.state!=AdapterState.READY:return
            yield item
            await asyncio.sleep(0)

def make_event(venue,symbol,kind,quote,payload,seq,domain,stamp,eid):
    ins=InstrumentId(venue,kind,symbol,quote)
    return EventEnvelope(venue,ins,EventType.TRADE,stamp,stamp+10,seq,domain,payload,eid)

def test_interleaved_multi_venue_events_are_journaled_and_analysed(tmp_path):
    async def run():
        nse=make_event("NSE","NIFTY",InstrumentKind.NSE_SPOT,"INR",TradeEvent(24000,2,"BUY"),1,"NSE:T",100,"nse1")
        btc=make_event("BINANCE","BTCUSDT",InstrumentKind.CRYPTO_PERPETUAL,"USDT",TradeEvent(35000,.01,"SELL"),1,"BINANCE:T",101,"btc1")
        a,b=FakeAdapter("NSE",[nse]),FakeAdapter("BINANCE",[btc])
        journal=EventJournal(tmp_path/"events.jsonl")
        registry=AnalyticsRegistry(tick_size_by_instrument={nse.instrument.canonical_id:.05,btc.instrument.canonical_id:.1})
        pipe=LiveIngestPipeline([a,b],journal=journal,validator=EventValidator(journal),analytics=registry)
        await pipe.run()
        assert pipe.stats().accepted==2
        assert {x.venue for x in registry.snapshots()}=={"NSE","BINANCE"}
        assert a.disconnected and b.disconnected
        rows=[json.loads(x) for x in (tmp_path/"events.jsonl").read_text().splitlines()]
        assert sum(x.get("record_type")=="event" for x in rows)==2
    asyncio.run(run())

def test_duplicate_event_rejected_without_double_count(tmp_path):
    async def run():
        event=make_event("BINANCE","BTCUSDT",InstrumentKind.CRYPTO_PERPETUAL,"USDT",TradeEvent(35000,.01,"BUY"),1,"B:T",100,"dup")
        adapter=FakeAdapter("BINANCE",[event,event]);journal=EventJournal(tmp_path/"events.jsonl")
        registry=AnalyticsRegistry(tick_size_by_instrument={event.instrument.canonical_id:.1})
        pipe=LiveIngestPipeline([adapter],journal=journal,validator=EventValidator(journal),analytics=registry)
        await pipe.run()
        assert (pipe.stats().accepted,pipe.stats().rejected)==(1,1)
        assert registry.state_for("BINANCE",event.instrument.canonical_id).snapshot().event_count==1
    asyncio.run(run())

def test_nse_mode4_snapshot_remains_unsequenced(tmp_path):
    nse=InstrumentId("NSE",InstrumentKind.NSE_SPOT,"NIFTY","INR")
    snap=DepthSnapshotEvent((DepthSnapshotLevel(100,1),),(DepthSnapshotLevel(101,1),),None)
    event=EventEnvelope("NSE",nse,EventType.DEPTH_SNAPSHOT,None,123,None,None,snap,"mode4")
    manager=DepthRecoveryManager()
    status=SnapshotSyncCoordinator(manager,EventJournal(tmp_path/"journal.jsonl")).apply(event)
    assert status.snapshot_sequence is None and not status.sequence_tracked
    assert manager.health("NSE",nse.canonical_id,"NSE:UNSEQUENCED_DEPTH")==DepthHealth.SYNCHRONIZED

def test_atomic_depth_batch_gap_and_snapshot_recovery(tmp_path):
    btc=InstrumentId("BINANCE",InstrumentKind.CRYPTO_PERPETUAL,"BTCUSDT","USDT")
    manager=DepthRecoveryManager(EventJournal(tmp_path/"depth.jsonl"))
    snap=DepthSnapshotEvent((DepthSnapshotLevel(100,1),),(DepthSnapshotLevel(101,1),),100)
    manager.apply_snapshot("BINANCE",btc.canonical_id,"B:DEPTH",snap)
    batch=DepthDeltaBatchEvent((DepthDeltaChange("BID",100,2),DepthDeltaChange("ASK",102,1)),101,102,100)
    result=manager.apply_batch("BINANCE",btc.canonical_id,"B:DEPTH",batch)
    assert result.accepted and manager.book("BINANCE",btc.canonical_id,"B:DEPTH")["last_sequence"]==102
    gap=DepthDeltaBatchEvent((DepthDeltaChange("BID",99,1),),105,105,103)
    result=manager.apply_batch("BINANCE",btc.canonical_id,"B:DEPTH",gap)
    assert not result.accepted and result.health==DepthHealth.DESYNCHRONIZED
    fresh=DepthSnapshotEvent((DepthSnapshotLevel(99,1),),(DepthSnapshotLevel(103,1),),105)
    event=EventEnvelope("BINANCE",btc,EventType.DEPTH_SNAPSHOT,None,600,105,"B:DEPTH",fresh,"fresh")
    status=SnapshotSyncCoordinator(manager).apply(event)
    assert status.recovered and manager.health("BINANCE",btc.canonical_id,"B:DEPTH")==DepthHealth.SYNCHRONIZED

def test_clean_shutdown_disconnects_adapters(tmp_path):
    async def run():
        btc=InstrumentId("BINANCE",InstrumentKind.CRYPTO_PERPETUAL,"BTCUSDT","USDT")
        class Blocking(FakeAdapter):
            async def events(self):
                if self.state!=AdapterState.READY:raise RuntimeError("not READY")
                await asyncio.Event().wait()
                if False:yield None
        adapter=Blocking("BLOCK",[])
        pipe=LiveIngestPipeline([adapter],journal=EventJournal(tmp_path/"shutdown.jsonl"),
            validator=EventValidator(),analytics=AnalyticsRegistry(tick_size_by_instrument={btc.canonical_id:.1}))
        await pipe.start();await asyncio.sleep(0);await pipe.stop()
        assert adapter.state==AdapterState.DISCONNECTED and adapter.disconnected
        assert pipe.stats().errors==0
    asyncio.run(run())
