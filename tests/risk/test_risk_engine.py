from dataclasses import FrozenInstanceError, replace
import pytest

from core.risk.contracts import DecisionReason, DecisionStatus
from core.risk.engine import RiskDecisionEngine
from core.risk.policy import DEFAULT_RISK_POLICIES
from core.signals.contracts import SignalDirection


def test_healthy_signal_approved_and_execution_disabled(policy, fusion, context):
    result = RiskDecisionEngine(policy).evaluate(fusion, context)
    assert result.decision.status is DecisionStatus.APPROVED
    assert result.decision.execution_mode == "DISABLED"
    assert result.decision.source_event_ids == ("evt-1", "evt-2")


@pytest.mark.parametrize("field,value,reason", [
    ("feed_status", "STALE", DecisionReason.FEED_STALE),
    ("book_synchronized", False, DecisionReason.BOOK_DESYNCHRONIZED),
    ("history_ready", False, DecisionReason.HISTORY_INSUFFICIENT),
])
def test_critical_integrity_failures_reject(policy, fusion, context, field, value, reason):
    result = RiskDecisionEngine(policy).evaluate(fusion, replace(context, **{field: value}))
    assert result.decision.status is DecisionStatus.REJECTED
    assert result.assessment.hard_veto
    assert reason in result.decision.risk.reasons


@pytest.mark.parametrize("field,reason", [
    ("liquidity_spread_ticks", DecisionReason.LIQUIDITY_DATA_UNAVAILABLE),
    ("exposure_utilization", DecisionReason.EXPOSURE_DATA_UNAVAILABLE),
    ("market_risk", DecisionReason.RISK_INPUT_INVALID),
    ("vpin_risk", DecisionReason.RISK_INPUT_INVALID),
    ("gex_age_ns", DecisionReason.GEX_CONTEXT_STALE),
])
def test_missing_critical_metrics_hold_not_approve(policy, fusion, context, field, reason):
    result = RiskDecisionEngine(policy).evaluate(fusion, replace(context, **{field: None}))
    assert result.decision.status is DecisionStatus.HOLD
    assert reason in result.decision.risk.reasons


def test_instrument_mismatch_hard_reject(policy, fusion, context):
    bad = replace(fusion.signal, instrument_id="BINANCE:CRYPTO_PERPETUAL:BTCUSDT:USDT")
    result = RiskDecisionEngine(policy).evaluate(replace(fusion, signal=bad), context)
    assert result.decision.status is DecisionStatus.REJECTED
    assert DecisionReason.INSTRUMENT_MISMATCH in result.decision.risk.reasons


def test_stale_signal_hard_reject(policy, fusion, context):
    result = RiskDecisionEngine(policy).evaluate(
        fusion, replace(context, now_receive_timestamp_ns=10_000_000_000)
    )
    assert result.decision.status is DecisionStatus.REJECTED
    assert DecisionReason.FEED_STALE in result.decision.risk.reasons


def test_neutral_signal_holds(policy, fusion, context):
    neutral = replace(fusion.signal, direction=SignalDirection.NEUTRAL)
    result = RiskDecisionEngine(policy).evaluate(replace(fusion, signal=neutral), context)
    assert result.decision.status is DecisionStatus.HOLD
    assert DecisionReason.DIRECTION_NEUTRAL in result.decision.risk.reasons


def test_hard_veto_precedes_soft_score(policy, fusion, context):
    result = RiskDecisionEngine(policy).evaluate(
        fusion, replace(context, book_synchronized=False, market_risk=0.99)
    )
    assert result.decision.status is DecisionStatus.REJECTED
    assert result.assessment.hard_veto


def test_upstream_fusion_is_not_mutated(policy, fusion, context):
    before = fusion
    RiskDecisionEngine(policy).evaluate(fusion, context)
    assert fusion is before
    assert fusion.accepted is True
    assert fusion.signal.direction is SignalDirection.LONG


def test_clock_domains_remain_separate(policy, fusion, context):
    result = RiskDecisionEngine(policy).evaluate(fusion, context)
    assert result.decision.exchange_timestamp_ns == 1_000_000_000
    assert result.decision.receive_timestamp_ns == 1_000_000_100
    assert result.decision.created_at_receive_timestamp_ns == context.now_receive_timestamp_ns


def test_account_limits_are_hard_vetoes(policy, fusion, context):
    result = RiskDecisionEngine(policy).evaluate(
        fusion, replace(context, exposure_utilization=0.95)
    )
    assert result.decision.status is DecisionStatus.REJECTED
    assert DecisionReason.EXPOSURE_LIMIT_BREACHED in result.decision.risk.reasons


def test_risk_contracts_are_immutable(policy, fusion, context):
    result = RiskDecisionEngine(policy).evaluate(fusion, context)
    with pytest.raises((FrozenInstanceError, AttributeError)):
        result.decision.status = DecisionStatus.REJECTED
