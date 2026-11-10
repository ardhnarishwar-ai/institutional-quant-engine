from dataclasses import replace
import pytest

from core.risk.contracts import DecisionStatus
from core.risk.engine import RiskDecisionEngine


@pytest.mark.parametrize("market,vpin,spread,depth", [
    (0.0, 0.0, 0.0, 100.0),
    (0.1, 0.2, 1.0, 10.0),
    (0.4, 0.5, 2.0, 2.0),
    (0.8, 0.7, 3.0, 1.0),
])
def test_risk_score_is_bounded(policy, fusion, context, market, vpin, spread, depth):
    result = RiskDecisionEngine(policy).evaluate(
        fusion,
        replace(context, market_risk=market, vpin_risk=vpin,
                liquidity_spread_ticks=spread, liquidity_depth_units=depth),
    )
    assert 0.0 <= result.assessment.risk_score <= 1.0


@pytest.mark.parametrize("age", [0, 1, 100, 1_000_000, 2_000_000_001])
def test_feed_age_never_creates_negative_risk_or_approval_after_stale(policy, fusion, context, age):
    result = RiskDecisionEngine(policy).evaluate(fusion, replace(context, feed_age_ns=age))
    assert 0.0 <= result.assessment.risk_score <= 1.0
    if age > policy.max_feed_age_ns:
        assert result.decision.status is DecisionStatus.REJECTED


def test_missing_account_metrics_never_approve(policy, fusion, context):
    result = RiskDecisionEngine(policy).evaluate(
        fusion, replace(context, exposure_utilization=None, daily_loss_utilization=None)
    )
    assert result.decision.status is DecisionStatus.HOLD
