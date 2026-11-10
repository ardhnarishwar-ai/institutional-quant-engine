import asyncio
import json
import pytest
from adapters.nse.smartapi import ExchangeTokens, SmartAPIConfig, SmartAPIWebSocket, Subscription

def config():
    return SmartAPIConfig(jwt_token="jwt", api_key="key", client_code="CLIENT",
                          feed_token="feed", heartbeat_interval_seconds=10,
                          retry_base_seconds=0, max_retries=1)

class Socket:
    def __init__(self, fail_recv=False):
        self.sent, self.closed, self.fail_recv = [], False, fail_recv
    async def send(self, data):
        self.sent.append(data)
    async def recv(self):
        if self.fail_recv:
            raise OSError("simulated transport drop")
        return b"frame"
    async def close(self):
        self.closed = True

class Connector:
    def __init__(self):
        self.sockets = []
    async def __call__(self, uri, **kwargs):
        sock = Socket(fail_recv=not self.sockets)
        self.sockets.append(sock)
        return sock

def test_transport_drop_reconnects_and_replays_subscription():
    async def run():
        connector = Connector()
        client = SmartAPIWebSocket(config(), connector=connector)
        sub = Subscription(1, (ExchangeTokens(1, ("26000",)),))
        await client.connect()
        await client.subscribe(sub)
        with pytest.raises(OSError, match="simulated transport drop"):
            await client.recv()
        await client.reconnect()
        assert len(connector.sockets) == 2
        assert connector.sockets[0].closed
        assert json.loads(connector.sockets[1].sent[0])["action"] == 1
        assert len(client.desired_subscriptions) == 1
        assert await client.recv() == b"frame"
        await client.disconnect()
    asyncio.run(run())
