"""Deterministic property-style boundary tests; no optional Hypothesis dependency."""

from decimal import Decimal

import pytest

from core.paper.contracts import PaperOrder, PaperOrderType


@pytest.mark.parametrize("qty", [Decimal("0.0001"), Decimal("1"), Decimal("1000000")])
def test_positive_quantities_round_trip_exactly(qty):
    item = PaperOrder(
        order_id="o", decision_id="d", venue="BINANCE",
        instrument_id="BINANCE:CRYPTO_PERPETUAL:BTCUSDT:USDT",
        direction="LONG", order_type=PaperOrderType.LIMIT,
        limit_price=Decimal("65000.01"), quantity=qty,
        created_exchange_timestamp_ns=1, created_receive_timestamp_ns=2,
        created_simulation_timestamp_ns=3, source_event_ids=("e",),
    )
    assert item.quantity == qty


@pytest.mark.parametrize("price", [0, -0.01, float("nan"), float("inf")])
def test_nonpositive_or_nonfinite_prices_are_rejected(price):
    with pytest.raises(ValueError):
        PaperOrder(
            order_id="o", decision_id="d", venue="NSE", instrument_id="NIFTY",
            direction="LONG", order_type=PaperOrderType.LIMIT, limit_price=price,
            quantity=1, created_exchange_timestamp_ns=None,
            created_receive_timestamp_ns=2, created_simulation_timestamp_ns=3,
            source_event_ids=("e",),
        )
