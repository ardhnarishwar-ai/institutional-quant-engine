import pytest
from adapters.base.interface import AdapterState, BaseAdapter, InvalidStateTransition


class DummyAdapter(BaseAdapter):
    def __init__(self, fail_sync=False):
        super().__init__("dummy")
        self.fail_sync = fail_sync

    async def connect(self):
        pass

    async def synchronize(self):
        if self.fail_sync:
            raise RuntimeError("sync failed")

    async def disconnect(self):
        pass

    async def events(self):
        if False:
            yield None


def test_valid_lifecycle():
    import asyncio
    adapter = DummyAdapter()
    asyncio.run(adapter.start())
    assert adapter.state == AdapterState.READY
    asyncio.run(adapter.mark_stale())
    assert adapter.state == AdapterState.STALE
    asyncio.run(adapter.stop())
    assert adapter.state == AdapterState.DISCONNECTED


def test_invalid_transition_rejected():
    adapter = DummyAdapter()
    with pytest.raises(InvalidStateTransition):
        adapter.transition(AdapterState.READY)


def test_failed_sync_returns_to_disconnected():
    import asyncio
    adapter = DummyAdapter(fail_sync=True)
    with pytest.raises(RuntimeError):
        asyncio.run(adapter.start())
    assert adapter.state == AdapterState.DISCONNECTED
