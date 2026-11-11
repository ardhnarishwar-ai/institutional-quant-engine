from __future__ import annotations

import ast
from pathlib import Path

import pytest

from adapters.execution import DeadEndExecutionAdapter, ExecutionDisabledError
from adapters.base.interface import BaseAdapter
from adapters.nse.smartapi.adapter import SmartAPILiveAdapter
from adapters.crypto.binance_usdm.adapter import BinanceUSDMLiveAdapter


ROOT = Path(__file__).resolve().parents[2]
ADAPTER_PATHS = (
    ROOT / "adapters/nse/smartapi/adapter.py",
    ROOT / "adapters/crypto/binance_usdm/adapter.py",
)
FORBIDDEN_ROUTING_NAMES = {
    "place_order", "submit_order", "create_order", "cancel_order",
    "modify_order", "replace_order", "new_order", "close_position",
}
SEALED_PATHS = (
    ROOT / "core/signals/contracts.py",
    ROOT / "core/signals/fusion.py",
    ROOT / "core/risk/contracts.py",
    ROOT / "core/risk/engine.py",
    ROOT / "core/paper/contracts.py",
    ROOT / "core/paper/policies.py",
    ROOT / "core/paper/eligibility.py",
    ROOT / "pipeline/event_validator.py",
    ROOT / "pipeline/live_ingest.py",
    ROOT / "pipeline/paper_trading.py",
)


def _defined_methods(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            names.add(node.name)
    return names


def test_dead_end_adapter_is_not_a_live_feed_adapter():
    adapter = DeadEndExecutionAdapter()
    assert adapter.execution_mode.value == "DISABLED"
    assert not isinstance(adapter, BaseAdapter)


@pytest.mark.parametrize(
    "method,args",
    [
        ("submit_order", ({"symbol": "BTCUSDT"},)),
        ("cancel_order", ("order-1",)),
        ("replace_order", ("order-1", {"price": "1"})),
        ("close_position", ("BTCUSDT",)),
    ],
)
def test_every_execution_operation_fails_locally(method, args):
    with pytest.raises(ExecutionDisabledError, match="permanently DISABLED"):
        getattr(DeadEndExecutionAdapter(), method)(*args)


def test_execution_adapter_has_no_transport_or_network_configuration():
    adapter = DeadEndExecutionAdapter()
    forbidden = ("transport", "connection", "session", "client", "websocket_url", "api_key")
    assert not any(hasattr(adapter, name) for name in forbidden)


@pytest.mark.parametrize("path", ADAPTER_PATHS)
def test_exchange_adapters_define_no_order_routing_methods(path):
    methods = _defined_methods(path)
    assert not (methods & FORBIDDEN_ROUTING_NAMES)


def test_nse_adapter_contract_remains_ingestion_only():
    methods = _defined_methods(ADAPTER_PATHS[0])
    assert {"connect", "synchronize", "disconnect", "events"} <= methods
    assert not (methods & FORBIDDEN_ROUTING_NAMES)


def test_binance_adapter_contract_remains_ingestion_only():
    methods = _defined_methods(ADAPTER_PATHS[1])
    assert {"connect", "synchronize", "disconnect", "decode_frame", "ingest", "events"} <= methods
    assert not (methods & FORBIDDEN_ROUTING_NAMES)


def test_live_ingest_execution_mode_is_disabled():
    source = (ROOT / "pipeline/live_ingest.py").read_text(encoding="utf-8")
    assert 'execution_mode = "DISABLED"' in source


def test_signal_risk_and_paper_contracts_retain_disabled_execution_lock():
    sources = [
        (ROOT / "core/signals/contracts.py").read_text(encoding="utf-8"),
        (ROOT / "core/signals/fusion.py").read_text(encoding="utf-8"),
        (ROOT / "core/risk/contracts.py").read_text(encoding="utf-8"),
        (ROOT / "core/risk/engine.py").read_text(encoding="utf-8"),
        (ROOT / "core/paper/policies.py").read_text(encoding="utf-8"),
        (ROOT / "core/paper/eligibility.py").read_text(encoding="utf-8"),
    ]
    combined = "\n".join(sources)
    assert "ExecutionMode.DISABLED" in combined
    assert '"DISABLED"' in combined


def test_sealed_production_paths_exist_and_are_not_part_of_stage_patch():
    for path in SEALED_PATHS:
        assert path.is_file(), str(path)
    # The test deliberately inspects contracts without importing or changing them.


def test_dead_end_methods_are_only_local_rejections():
    tree = ast.parse((ROOT / "adapters/execution/dead_end.py").read_text(encoding="utf-8"))
    calls = {
        node.func.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }
    forbidden_calls = {"urlopen", "create_connection", "connect", "request", "post", "put"}
    assert not (calls & forbidden_calls)
