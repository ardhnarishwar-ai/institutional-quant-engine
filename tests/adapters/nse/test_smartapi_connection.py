import asyncio
import json

import pytest

from adapters.nse.smartapi import (
    ExchangeTokens, SmartAPIConfig, SmartAPIConnectionError,
    SmartAPIWebSocket, Subscription,
)


def config(**kwargs):
    values = dict(
        jwt_token="jwt-test", api_key="api-test",
        client_code="CLIENT1", feed_token="feed-test",
        heartbeat_interval_seconds=0.01,
        retry_base_seconds=0,
    )
    values.update(kwargs)
    return SmartAPIConfig(**values)


class FakeSocket:
    def __init__(self):
        self.sent = []
        self.frames = asyncio.Queue()
        self.closed = False

    async def send(self, data):
        self.sent.append(data)

    async def recv(self):
        return await self.frames.get()

    async def close(self):
        self.closed = True


class Connector:
    def __init__(self, failures=0):
        self.failures = failures
        self.calls = []

    async def __call__(self, uri, **kwargs):
        self.calls.append((uri, kwargs))
        if len(self.calls) <= self.failures:
            raise OSError("temporary connect failure")
        return FakeSocket()


def test_connect_uses_secure_url_and_required_auth_headers():
    async def run():
        connector = Connector()
        client = SmartAPIWebSocket(config(), connector=connector)
        await client.connect()
        uri, kwargs = connector.calls[0]
        assert uri == "wss://smartapisocket.angelone.in/smart-stream"
        assert kwargs["additional_headers"] == {
            "Authorization": "jwt-test",
            "x-api-key": "api-test",
            "x-client-code": "CLIENT1",
            "x-feed-token": "feed-test",
        }
        assert kwargs["ping_interval"] is None
        await client.disconnect()
    asyncio.run(run())


def test_connect_retries_with_bounded_attempts():
    async def run():
        connector = Connector(failures=2)
        client = SmartAPIWebSocket(config(max_retries=2), connector=connector)
        await client.connect()
        assert len(connector.calls) == 3
        await client.disconnect()
    asyncio.run(run())


def test_connect_failure_reports_error_without_leaking_credentials():
    async def run():
        connector = Connector(failures=9)
        client = SmartAPIWebSocket(config(max_retries=1), connector=connector)
        with pytest.raises(SmartAPIConnectionError) as error:
            await client.connect()
        assert "jwt-test" not in str(error.value)
        assert not client.connected
    asyncio.run(run())


def test_subscribe_and_unsubscribe_send_documented_json_contract():
    async def run():
        connector = Connector()
        client = SmartAPIWebSocket(config(), connector=connector)
        await client.connect()
        sub = Subscription(1, (ExchangeTokens(1, ("26000", "26009")),))
        await client.subscribe(sub, "NIFTY01")
        subscribe = json.loads(connector.calls and client._ws.sent[-1])
        assert subscribe == {
            "correlationID": "NIFTY01",
            "action": 1,
            "params": {
                "mode": 1,
                "tokenList": [{"exchangeType": 1, "tokens": ["26000", "26009"]}],
            },
        }
        assert len(client.desired_subscriptions) == 1
        await client.unsubscribe(sub, "NIFTY02")
        unsubscribe = json.loads(client._ws.sent[-1])
        assert unsubscribe["action"] == 0
        assert len(client.desired_subscriptions) == 0
        await client.disconnect()
    asyncio.run(run())


def test_heartbeat_sends_text_ping_and_disconnect_cancels_task():
    async def run():
        connector = Connector()
        client = SmartAPIWebSocket(config(heartbeat_interval_seconds=0.005), connector=connector)
        await client.connect()
        await asyncio.sleep(0.02)
        assert "ping" in client._ws.sent
        await client.disconnect()
        assert client._ws is None
    asyncio.run(run())


def test_recv_discards_pong_and_returns_binary_frame():
    async def run():
        connector = Connector()
        client = SmartAPIWebSocket(config(), connector=connector)
        await client.connect()
        await client._ws.frames.put("pong")
        await client._ws.frames.put(b"\x01\x02")
        assert await client.recv() == b"\x01\x02"
        await client.disconnect()
    asyncio.run(run())


def test_reconnect_replays_desired_subscriptions():
    async def run():
        connector = Connector()
        client = SmartAPIWebSocket(config(), connector=connector)
        await client.connect()
        sub = Subscription(2, (ExchangeTokens(2, ("999",)),))
        await client.subscribe(sub)
        first_socket = client._ws
        await client.reconnect()
        assert first_socket.closed
        assert len(connector.calls) == 2
        assert len(client.desired_subscriptions) == 1
        assert json.loads(client._ws.sent[0])["action"] == 1
        await client.disconnect()
    asyncio.run(run())


def test_depth_mode_constraints_and_session_quota():
    with pytest.raises(ValueError, match="exchange type 1"):
        Subscription(4, (ExchangeTokens(2, ("123",)),))
    with pytest.raises(ValueError, match="50 tokens"):
        Subscription(4, (ExchangeTokens(1, tuple(str(i) for i in range(51))),))
    with pytest.raises(ValueError, match="1000"):
        # Build two distinct mode/token requests whose combined subscriptions exceed quota.
        async def run():
            connector = Connector()
            client = SmartAPIWebSocket(config(), connector=connector)
            await client.connect()
            groups = (ExchangeTokens(1, tuple(str(i) for i in range(1, 501))),)
            await client.subscribe(Subscription(1, groups))
            await client.subscribe(Subscription(2, groups))
            await client.subscribe(Subscription(3, groups))
        asyncio.run(run())


def test_subscription_validation_and_correlation_id():
    with pytest.raises(ValueError):
        ExchangeTokens(1, ("not-a-token",))
    with pytest.raises(ValueError):
        Subscription(9, (ExchangeTokens(1, ("1",)),))
    with pytest.raises(ValueError):
        Subscription(1, (ExchangeTokens(1, ("1",)),)).payload(action=1, correlation_id="too-long-123")


def test_context_manager_closes_socket():
    async def run():
        connector = Connector()
        client = SmartAPIWebSocket(config(), connector=connector)
        async with client:
            socket = client._ws
            assert client.connected
        assert socket.closed
        assert not client.connected
    asyncio.run(run())
