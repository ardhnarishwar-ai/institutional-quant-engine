import asyncio

import pytest

from adapters.base.interface import (
    AdapterState,
    BaseAdapter,
    InvalidStateTransition,
)


class DummyAdapter(BaseAdapter):
    def __init__(self, *, connect_error=None, sync_error=None,
                 disconnect_error=None, event_error=None):
        super().__init__("dummy")
        self.connect_error = connect_error
        self.sync_error = sync_error
        self.disconnect_error = disconnect_error
        self.event_error = event_error
        self.connect_calls = 0
        self.sync_calls = 0
        self.disconnect_calls = 0
        self.resource_open = False

    async def connect(self):
        self.connect_calls += 1
        self.resource_open = True
        if self.connect_error:
            raise self.connect_error

    async def synchronize(self):
        self.sync_calls += 1
        if self.sync_error:
            raise self.sync_error

    async def disconnect(self):
        self.disconnect_calls += 1
        self.resource_open = False
        if self.disconnect_error:
            raise self.disconnect_error

    async def events(self):
        if self.event_error:
            raise self.event_error
        if False:
            yield None


def test_successful_start_and_stop_tears_down_exactly_once():
    async def run():
        adapter = DummyAdapter()
        await adapter.start()
        assert adapter.state == AdapterState.READY
        assert adapter.resource_open
        await adapter.stop()
        assert adapter.state == AdapterState.DISCONNECTED
        assert not adapter.resource_open
        assert adapter.disconnect_calls == 1
    asyncio.run(run())


def test_sync_failure_disconnects_after_successful_connect():
    async def run():
        error = RuntimeError("snapshot synchronization failed")
        adapter = DummyAdapter(sync_error=error)
        with pytest.raises(RuntimeError, match="snapshot synchronization failed"):
            await adapter.start()
        assert adapter.connect_calls == 1
        assert adapter.sync_calls == 1
        assert adapter.disconnect_calls == 1
        assert not adapter.resource_open
        assert adapter.state == AdapterState.DISCONNECTED
    asyncio.run(run())


def test_unexpected_exception_during_sync_still_disconnects():
    async def run():
        adapter = DummyAdapter(sync_error=ValueError("unexpected decoder failure"))
        with pytest.raises(ValueError, match="unexpected decoder failure"):
            await adapter.start()
        assert adapter.disconnect_calls == 1
        assert adapter.state == AdapterState.DISCONNECTED
        assert not adapter.resource_open
    asyncio.run(run())


def test_connect_failure_does_not_call_disconnect_without_successful_connect():
    async def run():
        adapter = DummyAdapter(connect_error=OSError("socket open failed"))
        with pytest.raises(OSError, match="socket open failed"):
            await adapter.start()
        assert adapter.disconnect_calls == 0
        assert adapter.state == AdapterState.DISCONNECTED
    asyncio.run(run())


def test_cleanup_failure_does_not_mask_original_sync_failure():
    async def run():
        adapter = DummyAdapter(
            sync_error=RuntimeError("primary sync failure"),
            disconnect_error=OSError("secondary close failure"),
        )
        with pytest.raises(RuntimeError, match="primary sync failure"):
            await adapter.start()
        assert adapter.disconnect_calls == 1
        assert adapter.state == AdapterState.DISCONNECTED
        assert not adapter.resource_open
    asyncio.run(run())


def test_stop_guarantees_disconnected_state_even_if_disconnect_raises():
    async def run():
        adapter = DummyAdapter(disconnect_error=OSError("close failed"))
        await adapter.start()
        with pytest.raises(OSError, match="close failed"):
            await adapter.stop()
        assert adapter.disconnect_calls == 1
        assert adapter.state == AdapterState.DISCONNECTED
    asyncio.run(run())


def test_runtime_exception_during_event_iteration_triggers_teardown():
    async def run():
        adapter = DummyAdapter(event_error=RuntimeError("stream decoder crashed"))
        await adapter.start()
        with pytest.raises(RuntimeError, match="stream decoder crashed"):
            async for _ in adapter.guarded_events():
                pass
        assert adapter.disconnect_calls == 1
        assert adapter.state == AdapterState.DISCONNECTED
        assert not adapter.resource_open
    asyncio.run(run())


def test_start_rejects_non_disconnected_state():
    async def run():
        adapter = DummyAdapter()
        await adapter.start()
        with pytest.raises(InvalidStateTransition):
            await adapter.start()
        await adapter.stop()
    asyncio.run(run())
