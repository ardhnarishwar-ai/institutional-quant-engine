from dataclasses import FrozenInstanceError
from decimal import Decimal as D

import pytest

from core.paper.policies import (
    InstrumentPolicyGate,
    InstrumentPolicyRegistry,
    InstrumentSimulationPolicy,
    PolicyMarketContext,
    PolicyReason,
    PolicyStatus,
)


def policy(venue="NSE", instrument="NIFTY", **overrides):
    values = dict(
        venue=venue, instrument_id=instrument, policy_version="test-v1",
        tick_size=D("0.05"), quantity_step=D("1"), minimum_quantity=D("1"),
        maximum_order_quantity=D("100"), maximum_position_quantity=D("200"),
        maximum_order_notional=D("1000000"), maximum_spread_bps=D("20"),
        maximum_participation_rate=D("0.10"), maximum_leverage=D("1"),
        allow_short=True,
    )
    values.update(overrides)
    return InstrumentSimulationPolicy(**values)


def context(**overrides):
    values = dict(
        feed_fresh=True, book_synchronized=True, best_bid=D("99.95"),
        best_ask=D("100.00"), displayed_opposite_depth=D("1000"),
        current_position_quantity=D("0"),
    )
    values.update(overrides)
    return PolicyMarketContext(**values)


def gate(*policies):
    return InstrumentPolicyGate(InstrumentPolicyRegistry(policies))


def test_valid_request_is_approved_and_policy_version_is_attached():
    result = gate(policy()).evaluate(
        venue="NSE", instrument_id="NIFTY", side="BUY",
        price=D("100"), quantity=D("10"), context=context(),
    )
    assert result.status is PolicyStatus.APPROVED
    assert result.policy_version == "test-v1"
    assert result.estimated_notional == D("1000")


def test_missing_policy_fails_closed_to_hold():
    result = gate(policy()).evaluate(
        venue="NSE", instrument_id="BANKNIFTY", side="BUY",
        price=D("100"), quantity=D("1"), context=context(),
    )
    assert result.status is PolicyStatus.HOLD
    assert PolicyReason.POLICY_NOT_FOUND in result.reasons


@pytest.mark.parametrize(("field", "value", "reason"), [
    ("feed_fresh", False, PolicyReason.STALE_FEED),
    ("book_synchronized", False, PolicyReason.DESYNCHRONIZED_BOOK),
    ("best_bid", None, PolicyReason.MISSING_MARKET_METRIC),
    ("displayed_opposite_depth", D("0"), PolicyReason.MISSING_MARKET_METRIC),
])
def test_missing_or_unsafe_market_state_holds(field, value, reason):
    result = gate(policy()).evaluate(
        venue="NSE", instrument_id="NIFTY", side="BUY",
        price=D("100"), quantity=D("1"), context=context(**{field: value}),
    )
    assert result.status is PolicyStatus.HOLD
    assert reason in result.reasons


def test_tick_step_position_and_notional_limits_reject():
    g = gate(policy())
    cases = [
        (D("100.01"), D("1"), D("0"), PolicyReason.PRICE_OFF_TICK),
        (D("100"), D("1.5"), D("0"), PolicyReason.QUANTITY_OFF_STEP),
        (D("100"), D("101"), D("0"), PolicyReason.ORDER_QUANTITY_LIMIT),
        (D("100"), D("10"), D("195"), PolicyReason.POSITION_LIMIT),
        (D("100000"), D("20"), D("0"), PolicyReason.NOTIONAL_LIMIT),
    ]
    for price, qty, position, reason in cases:
        result = g.evaluate(
            venue="NSE", instrument_id="NIFTY", side="BUY",
            price=price, quantity=qty,
            context=context(current_position_quantity=position),
        )
        assert result.status is PolicyStatus.REJECTED
        assert reason in result.reasons


def test_spread_and_participation_limits_hold():
    g = gate(policy(maximum_spread_bps=D("2")))
    wide = g.evaluate(
        venue="NSE", instrument_id="NIFTY", side="BUY",
        price=D("100"), quantity=D("1"),
        context=context(best_bid=D("99"), best_ask=D("101")),
    )
    assert wide.status is PolicyStatus.HOLD
    assert PolicyReason.SPREAD_LIMIT in wide.reasons

    thin = gate(policy()).evaluate(
        venue="NSE", instrument_id="NIFTY", side="BUY",
        price=D("100"), quantity=D("20"),
        context=context(displayed_opposite_depth=D("100")),
    )
    assert thin.status is PolicyStatus.HOLD
    assert PolicyReason.PARTICIPATION_LIMIT in thin.reasons


def test_execution_mode_cannot_be_enabled():
    result = gate(policy()).evaluate(
        venue="NSE", instrument_id="NIFTY", side="BUY",
        price=D("100"), quantity=D("1"), context=context(),
        execution_mode="LIVE",
    )
    assert result.status is PolicyStatus.REJECTED
    assert PolicyReason.EXECUTION_NOT_DISABLED in result.reasons


def test_registry_is_instrument_and_venue_isolated():
    g = gate(
        policy("NSE", "NIFTY"),
        policy("NSE", "BANKNIFTY"),
        policy("NSE", "FINNIFTY"),
        policy("BINANCE", "BTCUSDT"),
    )
    for venue, instrument in (
        ("NSE", "NIFTY"), ("NSE", "BANKNIFTY"),
        ("NSE", "FINNIFTY"), ("BINANCE", "BTCUSDT"),
    ):
        result = g.evaluate(
            venue=venue, instrument_id=instrument, side="BUY",
            price=D("100"), quantity=D("1"), context=context(),
        )
        assert result.status is PolicyStatus.APPROVED
        assert (result.venue, result.instrument_id) == (venue, instrument)


def test_policy_contracts_are_immutable_and_duplicate_keys_rejected():
    p = policy()
    with pytest.raises(FrozenInstanceError):
        p.tick_size = D("1")
    with pytest.raises(ValueError):
        InstrumentPolicyRegistry([p, p])


def test_policy_configuration_rejects_invalid_limits():
    with pytest.raises(ValueError):
        policy(tick_size=D("0"))
    with pytest.raises(ValueError):
        policy(maximum_participation_rate=D("1.1"))
    with pytest.raises(ValueError):
        policy(execution_mode="LIVE")
