class BinanceTransportError(ConnectionError): pass
class BinanceWebSocketTransport:
    def __init__(self,url,*,connect_factory=None):
        self.url,self._factory,self._socket=url,connect_factory,None
    async def connect(self):
        factory=self._factory
        if factory is None:
            try:
                from websockets.asyncio.client import connect as factory
            except ImportError:
                try: from websockets import connect as factory
                except ImportError as exc:
                    raise RuntimeError("Live feed requires: pip install 'websockets>=12,<16'") from exc
        self._socket=await factory(self.url,ping_interval=180,ping_timeout=600,max_size=2**20)
    async def recv(self):
        if self._socket is None: raise BinanceTransportError("transport disconnected")
        return await self._socket.recv()
    async def disconnect(self):
        socket,self._socket=self._socket,None
        if socket is not None: await socket.close()
