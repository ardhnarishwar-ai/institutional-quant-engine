from dataclasses import FrozenInstanceError
from decimal import Decimal

import pytest

from core.paper.contracts import (
    ExecutionSource, PaperAccount, PaperFill, PaperOrder, PaperOrderStatus,
    PaperOrderType, PaperPosition,
)


def order(**overrides):
    values = dict(
        order_id="po-1", decision_id="d-1", venue="NSE",
        instrument_id="NSE:NSE_SPOT:NIFTY:INR", direction="LONG",
        order_type=PaperOrderType.LIMIT, limit_price=Decimal("100"),
        quantity=Decimal("2"), created_exchange_timestamp_ns=None,
        created_receive_timestamp_ns=20, created_simulation_timestamp_ns=21,
        source_event_ids=("evt-1",),
    )
    values.update(overrides)
    return PaperOrder(**values)


def fill(**overrides):
    values = dict(
        fill_id="f-1", order_id="po-1", venue="NSE",
        instrument_id="NSE:NSE_SPOT:NIFTY:INR", side="BUY",
        price=Decimal("100"), quantity=Decimal("1"),
        execution_source=ExecutionSource.PAPER_SIMULATOR,
        market_evidence_event_ids=("trade-1", "depth-1"),
        fill_model_version="depth-fill-v1", exchange_timestamp_ns=None,
        receive_timestamp_ns=30, simulation_timestamp_ns=31,
    )
    values.update(overrides)
    return PaperFill(**values)


def test_order_is_immutable_and_preserves_nullable_exchange_clock():
    item = order()
    assert item.created_exchange_timestamp_ns is None
    assert item.created_receive_timestamp_ns == 20
    with pytest.raises(FrozenInstanceError):
        item.quantity = Decimal("3")


@pytest.mark.parametrize("quantity", [0, -1, "NaN", "Infinity", True])
def test_order_rejects_invalid_quantity(quantity):
    with pytest.raises(ValueError):
        order(quantity=quantity)


def test_order_rejects_empty_provenance():
    with pytest.raises(ValueError):
        order(source_event_ids=())


def test_fill_enforces_paper_source_and_audit_clocks():
    item = fill()
    assert item.execution_source is ExecutionSource.PAPER_SIMULATOR
    assert item.exchange_timestamp_ns is None
    assert item.receive_timestamp_ns == 30
    assert item.simulation_timestamp_ns == 31
    with pytest.raises(ValueError):
        fill(execution_source="LIVE_BROKER")
    with pytest.raises(ValueError):
        fill(market_evidence_event_ids=())


def test_fill_rejects_synthetic_or_invalid_clock_values():
    with pytest.raises(ValueError):
        fill(receive_timestamp_ns=None)
    with pytest.raises(ValueError):
        fill(exchange_timestamp_ns=-1)


def test_position_and_account_are_immutable_value_objects():
    position = PaperPosition("NSE", "NIFTY", Decimal("2"), Decimal("100"))
    account = PaperAccount("paper-1", "INR", Decimal("10000"), Decimal("10000"))
    assert position.quantity == Decimal("2")
    assert account.cash == Decimal("10000")
    with pytest.raises(FrozenInstanceError):
        account.cash = Decimal("0")


def test_order_status_and_order_type_are_explicit_enums():
    assert PaperOrderStatus.CREATED.value == "CREATED"
    assert PaperOrderType.MARKETABLE_LIMIT.value == "MARKETABLE_LIMIT"
    with pytest.raises(ValueError):
        order(status="FILLED")
