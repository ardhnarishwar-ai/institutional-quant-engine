import asyncio,json,pytest
from adapters.base.events import EventType
from adapters.base.interface import AdapterState
from adapters.crypto.binance_usdm.adapter import BinanceFrameError,BinanceUSDMLiveAdapter
from adapters.crypto.binance_usdm.config import BinanceUSDMConfig

class FakeTransport:
    def __init__(self): self.connected=False
    async def connect(self): self.connected=True
    async def recv(self): await asyncio.Future()
    async def disconnect(self): self.connected=False

def trade(m=True):
    return {"stream":"btcusdt@aggTrade","data":{"e":"aggTrade","E":1700000000123,"s":"BTCUSDT","a":101,"p":"35000.25","q":"0.004","T":1700000000120,"m":m}}
def depth(U=101,u=102,pu=100):
    return {"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1700000000124,"T":1700000000122,"s":"BTCUSDT","U":U,"u":u,"pu":pu,"b":[["35000","1.25"],["34999","0"]],"a":[["35001","2.5"]]}}
def snap(last=100): return {"lastUpdateId":last,"bids":[["34999","2"],["34998","1"]],"asks":[["35001","3"],["35002","1"]]}
async def provider(symbol,limit): return snap()
def make(**kw): return BinanceUSDMLiveAdapter(transport=kw.pop("transport",FakeTransport()),receive_clock=lambda:1800000000000000000,monotonic_clock=lambda:99,**kw)

def test_config_stream_names_and_tls():
    c=BinanceUSDMConfig(); assert c.streams==("btcusdt@aggTrade","btcusdt@depth@100ms")
    assert c.websocket_url.startswith("wss://")
    with pytest.raises(ValueError): BinanceUSDMConfig(base_url="ws://bad")

def test_trade_maps_taker_side_and_distinct_clocks():
    e=make().decode_frame(json.dumps(trade()))[0]
    assert e.instrument.canonical_id=="BINANCE:CRYPTO_PERPETUAL:BTCUSDT:USDT"
    assert e.event_type==EventType.TRADE and e.payload.aggressor_side=="SELL"
    assert e.exchange_timestamp_ns==1700000000120000000
    assert e.receive_timestamp_ns==1800000000000000000
    assert e.sequence_domain.endswith(":AGGTRADE")

def test_buyer_taker_maps_buy():
    assert make().decode_frame(json.dumps(trade(False)))[0].payload.aggressor_side=="BUY"

def test_depth_delta_delete_and_own_domain():
    events=make().decode_frame(json.dumps(depth()))
    assert len(events) == 1
    batch = events[0].payload
    assert len(batch.changes) == 3
    assert batch.changes[0].side == "BID"
    assert batch.changes[1].action == "DELETE"
    assert events[0].sequence_domain.endswith(":DEPTH")

def test_strict_schema_symbol_and_numeric_validation():
    a=make()
    with pytest.raises(BinanceFrameError): a.decode_frame('{"e":"aggTrade","s":"BTCUSDT"}')
    bad=trade(); bad["data"]["s"]="ETHUSDT"
    with pytest.raises(BinanceFrameError): a.decode_frame(json.dumps(bad))
    bad=trade(); bad["data"]["p"]="NaN"
    with pytest.raises(BinanceFrameError): a.decode_frame(json.dumps(bad))

def test_rest_snapshot_is_initial_boundary_and_exchange_time_unverified():
    async def run():
        a=make(snapshot_provider=provider); await a.start(); e=await a._queue.get()
        assert e.event_type==EventType.DEPTH_SNAPSHOT and e.exchange_timestamp_ns is None
        assert e.payload.snapshot_sequence==100
        await a.stop()
    asyncio.run(run())

def test_depth_bridge_and_pu_chain():
    async def run():
        a=make(snapshot_provider=provider); await a.start(); await a._queue.get()
        await a.ingest(json.dumps(depth(101,102,99)))
        assert a._last_depth_update==102 and a._queue.qsize() == 1
        await a.ingest(json.dumps(depth(103,104,102)))
        assert a._last_depth_update==104 and a._queue.qsize() == 2
        await a.stop()
    asyncio.run(run())

def test_depth_gap_requests_snapshot_recovery():
    async def run():
        calls=[]
        async def snap_provider(symbol,limit):
            calls.append(1); return snap(100 if len(calls)==1 else 200)
        a=make(snapshot_provider=snap_provider); await a.start(); await a._queue.get()
        await a.ingest(json.dumps(depth(101,102,99)))
        await a.ingest(json.dumps(depth(110,111,109)))
        assert len(calls)==2 and a._last_depth_update==200 and a.sequence_gaps
        await a.stop()
    asyncio.run(run())

def test_without_snapshot_provider_depth_is_buffered():
    async def run():
        a=make(); await a.start(); await a.ingest(json.dumps(depth()))
        assert not a._depth_synced and len(a._pending_depth)==1
        await a.stop()
    asyncio.run(run())

def test_trade_and_depth_domains_are_isolated():
    a=make(); t=a.decode_frame(json.dumps(trade()))[0]; d=a.decode_frame(json.dumps(depth()))[0]
    assert t.sequence_domain!=d.sequence_domain

def test_journal_replay_round_trip(tmp_path):
    from pipeline.event_journal import EventJournal
    from adapters.replay.reader import ReplayReader
    e=make().decode_frame(json.dumps(trade()))[0]; path=tmp_path/"crypto.jsonl"
    EventJournal(path).append(e); replay=list(ReplayReader(path).events())
    assert replay==[e] and replay[0].exchange_timestamp_ns==e.exchange_timestamp_ns
    assert replay[0].receive_timestamp_ns==e.receive_timestamp_ns

def test_transport_lifecycle():
    async def run():
        tr=FakeTransport(); a=make(transport=tr); await a.start()
        assert tr.connected; await a.stop()
        assert not tr.connected and a.state==AdapterState.DISCONNECTED
    asyncio.run(run())
