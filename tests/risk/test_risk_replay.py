from dataclasses import replace
from core.risk.engine import RiskDecisionEngine


def test_repeated_risk_evaluation_is_deterministic(policy, fusion, context):
    engine = RiskDecisionEngine(policy)
    outputs = [engine.evaluate(fusion, context).decision for _ in range(10)]
    assert all(item == outputs[0] for item in outputs)


def test_replay_changes_only_when_context_changes(policy, fusion, context):
    engine = RiskDecisionEngine(policy)
    baseline = engine.evaluate(fusion, context).decision
    changed = engine.evaluate(fusion, replace(context, liquidity_spread_ticks=policy.max_spread_ticks + 1)).decision
    assert baseline != changed
