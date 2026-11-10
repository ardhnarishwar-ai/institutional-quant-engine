from decimal import Decimal as D

import pytest

from core.paper.costs import FeeSchedule, TransactionCostModel


def test_cost_components_and_total_are_deterministic():
    model = TransactionCostModel(FeeSchedule(
        fee_bps=D("10"), transaction_tax_bps=D("5"),
        exchange_bps=D("2"), regulatory_bps=D("1"), gst_bps=D("1800"),
    ), "INR")
    result = model.calculate(D("100"), D("10"))
    assert result.notional == D("1000")
    assert result.fee == D("1.00")
    assert result.transaction_tax == D("0.50")
    assert result.exchange_charge == D("0.20")
    assert result.regulatory_charge == D("0.10")
    assert result.gst == D("0.23")
    assert result.total == D("2.03")
    assert result.currency == "INR"


@pytest.mark.parametrize(("price", "qty"), [(0, 1), (1, 0), (-1, 1), (1, -1)])
def test_invalid_trade_values_rejected(price, qty):
    with pytest.raises(ValueError):
        TransactionCostModel(FeeSchedule()).calculate(D(str(price)), D(str(qty)))


def test_invalid_fee_schedule_rejected():
    with pytest.raises(ValueError):
        FeeSchedule(fee_bps=D("-1"))
    with pytest.raises(ValueError):
        FeeSchedule(currency_quantum=D("0"))


def test_rounding_uses_decimal_half_up():
    model = TransactionCostModel(FeeSchedule(fee_bps=D("5")))
    assert model.calculate(D("10"), D("1")).fee == D("0.01")
