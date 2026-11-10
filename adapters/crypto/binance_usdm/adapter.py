import asyncio,json,math,time
from typing import AsyncIterator
from adapters.base.events import DepthDeltaEvent,DepthSnapshotEvent,DepthSnapshotLevel,EventEnvelope,EventType,TradeEvent
from adapters.base.depth_batch import DepthDeltaBatchEvent
from adapters.base.instrument import InstrumentId,InstrumentKind
from adapters.base.interface import AdapterState,BaseAdapter
from adapters.crypto.binance_usdm.config import BinanceUSDMConfig
from adapters.crypto.binance_usdm.transport import BinanceWebSocketTransport

class BinanceFrameError(ValueError): pass
def _int(v,n):
    if isinstance(v,bool) or not isinstance(v,int) or v<0: raise BinanceFrameError(f"{n} must be non-negative integer")
    return v
def _num(v,n,zero=False):
    try: x=float(v)
    except (TypeError,ValueError) as exc: raise BinanceFrameError(f"{n} must be numeric") from exc
    if not math.isfinite(x) or (x<0 if zero else x<=0): raise BinanceFrameError(f"invalid {n}")
    return x

class BinanceUSDMLiveAdapter(BaseAdapter):
    # Public BTCUSDT USD-M feed; no credentials, account or order methods.
    def __init__(self,config=None,*,transport=None,snapshot_provider=None,receive_clock=time.time_ns,monotonic_clock=time.monotonic_ns):
        self.config=config or BinanceUSDMConfig()
        super().__init__("BINANCE-USD-M-BTCUSDT")
        self.instrument=InstrumentId("BINANCE",InstrumentKind.CRYPTO_PERPETUAL,"BTCUSDT","USDT")
        self.transport=transport or BinanceWebSocketTransport(self.config.websocket_url)
        self.snapshot_provider=snapshot_provider
        self.receive_clock,self.monotonic_clock=receive_clock,monotonic_clock
        self._queue=asyncio.Queue(); self._event_counter=0
        self._last_depth_update=None; self._depth_synced=False; self._depth_bridged=False; self._pending_depth=[]
        self._depth_ranges={}; self._last_exchange={}; self._last_receive={}; self._last_monotonic=None
        self._reader_task=None; self._stop=asyncio.Event(); self.sequence_gaps=[]
    @property
    def trade_sequence_domain(self): return "BINANCE:USD-M:BTCUSDT:AGGTRADE"
    @property
    def depth_sequence_domain(self): return "BINANCE:USD-M:BTCUSDT:DEPTH"
    async def connect(self): await self.transport.connect()
    async def synchronize(self):
        if self.snapshot_provider is not None:
            snap=await self.snapshot_provider("BTCUSDT",self.config.depth_limit)
            await self._queue.put(self._install_snapshot(snap))
    async def disconnect(self):
        self._stop.set()
        task,self._reader_task=self._reader_task,None
        if task is not None and task is not asyncio.current_task():
            task.cancel()
            try: await task
            except asyncio.CancelledError: pass
        await self.transport.disconnect()
    def _times(self,event_ms,transaction_ms,domain):
        ex=_int(transaction_ms if transaction_ms is not None else event_ms,"exchange time")*1_000_000
        rx,mono=self.receive_clock(),self.monotonic_clock()
        if isinstance(rx,bool) or not isinstance(rx,int) or rx<0: raise BinanceFrameError("invalid receive clock")
        if isinstance(mono,bool) or not isinstance(mono,int) or mono<0: raise BinanceFrameError("invalid monotonic clock")
        if self._last_monotonic is not None and mono<self._last_monotonic: raise BinanceFrameError("monotonic clock regressed")
        if ex<self._last_exchange.get(domain,0) or rx<self._last_receive.get(domain,0): raise BinanceFrameError("event clock regressed")
        self._last_monotonic=mono; self._last_exchange[domain]=ex; self._last_receive[domain]=rx
        return ex,rx
    def _event(self,typ,payload,ex,rx,seq,domain,label):
        self._event_counter+=1
        return EventEnvelope("BINANCE",self.instrument,typ,ex,rx,seq,domain,payload,f"binance-usdm-{self._event_counter}-{label}")
    def decode_frame(self,frame):
        if isinstance(frame,bytes):
            try: frame=frame.decode("utf-8")
            except UnicodeDecodeError as exc: raise BinanceFrameError("frame must be UTF-8") from exc
        try: root=json.loads(frame)
        except (TypeError,json.JSONDecodeError) as exc: raise BinanceFrameError("invalid JSON") from exc
        if not isinstance(root,dict): raise BinanceFrameError("frame must be object")
        data=root.get("data",root)
        if not isinstance(data,dict) or data.get("s")!="BTCUSDT": raise BinanceFrameError("unexpected symbol or payload")
        kind=data.get("e")
        if kind=="aggTrade":
            if any(k not in data for k in ("E","T","a","p","q","m")): raise BinanceFrameError("aggTrade missing fields")
            if not isinstance(data["m"],bool): raise BinanceFrameError("m must be boolean")
            seq=_int(data["a"],"aggregate trade id"); ex,rx=self._times(data["E"],data["T"],"trade")
            side="SELL" if data["m"] else "BUY"
            return [self._event(EventType.TRADE,TradeEvent(_num(data["p"],"price"),_num(data["q"],"quantity"),side),ex,rx,seq,self.trade_sequence_domain,"trade")]
        if kind=="depthUpdate":
            if any(k not in data for k in ("E","T","U","u","pu","b","a")): raise BinanceFrameError("depthUpdate missing fields")
            first,final,prev=(_int(data["U"],"U"),_int(data["u"],"u"),_int(data["pu"],"pu"))
            if final<first: raise BinanceFrameError("inverted depth range")
            ex,rx=self._times(data["E"],data["T"],"depth"); changes=[]
            for side,rows in (("BID",data["b"]),("ASK",data["a"])):
                if not isinstance(rows,list): raise BinanceFrameError("depth side must be array")
                for row in rows:
                    if not isinstance(row,list) or len(row)!=2: raise BinanceFrameError("depth level must be [price,qty]")
                    price,qty=_num(row[0],"price"),_num(row[1],"quantity",True)
                    changes.append(DepthDeltaEvent(side,price,qty,"DELETE" if qty==0 else "SET"))
            if not changes: raise BinanceFrameError("depth update must contain at least one level change")
            payload=DepthDeltaBatchEvent(tuple(changes),first,final,prev)
            return [self._event(EventType.DEPTH_DELTA_BATCH,payload,ex,rx,final,self.depth_sequence_domain,"depth")]
        raise BinanceFrameError(f"unsupported event type: {kind!r}")
    def _install_snapshot(self,snap):
        if not isinstance(snap,dict) or not {"lastUpdateId","bids","asks"}<=set(snap): raise BinanceFrameError("snapshot requires lastUpdateId/bids/asks")
        seq=_int(snap["lastUpdateId"],"lastUpdateId")
        bids=tuple(sorted((DepthSnapshotLevel(_num(x[0],"bid"),_num(x[1],"bid qty")) for x in snap["bids"]),key=lambda x:x.price,reverse=True))
        asks=tuple(sorted((DepthSnapshotLevel(_num(x[0],"ask"),_num(x[1],"ask qty")) for x in snap["asks"]),key=lambda x:x.price))
        payload=DepthSnapshotEvent(bids,asks,seq); self._last_depth_update=seq; self._depth_synced=True; self._depth_bridged=False
        rx=self.receive_clock()
        if isinstance(rx,bool) or not isinstance(rx,int) or rx<0: raise BinanceFrameError("invalid receive clock")
        return self._event(EventType.DEPTH_SNAPSHOT,payload,None,rx,seq,self.depth_sequence_domain,"snapshot")
    async def _recover(self,reason):
        self._depth_synced=False; self.sequence_gaps.append(reason)
        if self.snapshot_provider is None: return
        snap=await self.snapshot_provider("BTCUSDT",self.config.depth_limit)
        await self._queue.put(self._install_snapshot(snap))
        pending,self._pending_depth=self._pending_depth,[]
        for item in pending: await self._process_depth(item)
    async def _process_depth(self,data):
        first,final,prev=(_int(data["U"],"U"),_int(data["u"],"u"),_int(data["pu"],"pu"))
        if not self._depth_synced: self._pending_depth.append(data); return
        last=self._last_depth_update
        if final<=last: return
        # The first event bridges the REST snapshot by update range only.
        # Binance pu continuity applies to each event after that bridge.
        bridge_ok = first <= last + 1 <= final
        chain_ok = (not self._depth_bridged) or (prev == last)
        if not bridge_ok or not chain_ok:
            self._pending_depth.append(data); await self._recover(f"gap last={last}, U={first}, u={final}, pu={prev}"); return
        self._last_depth_update=final
        self._depth_bridged=True
        for event in self.decode_frame(json.dumps(data)): await self._queue.put(event)
    async def ingest(self,frame):
        try: root=json.loads(frame.decode() if isinstance(frame,bytes) else frame)
        except Exception as exc: raise BinanceFrameError("invalid JSON frame") from exc
        data=root.get("data",root) if isinstance(root,dict) else None
        if isinstance(data,dict) and data.get("e")=="depthUpdate": await self._process_depth(data)
        else:
            for event in self.decode_frame(frame): await self._queue.put(event)
    async def events(self)->AsyncIterator[EventEnvelope]:
        if self.state!=AdapterState.READY: raise RuntimeError("adapter must be READY")
        if self._reader_task is None: self._reader_task=asyncio.create_task(self._reader())
        while self.state==AdapterState.READY: yield await self._queue.get()
    async def _reader(self):
        try:
            while not self._stop.is_set(): await self.ingest(await self.transport.recv())
        except asyncio.CancelledError: raise
        except BaseException:
            if self.state==AdapterState.READY: await self.mark_stale()
            raise
