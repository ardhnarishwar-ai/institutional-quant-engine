from core.risk import DecisionGate, DecisionStatus, RiskContext


def test_decision_gate_facade(policy, fusion, context):
    result = DecisionGate(policy).evaluate(fusion, context)
    assert result.decision.status is DecisionStatus.APPROVED


def test_rejected_fusion_never_approved(policy, fusion, context):
    from dataclasses import replace
    result = DecisionGate(policy).evaluate(replace(fusion, accepted=False, signal=None), context)
    assert result.decision.status is DecisionStatus.REJECTED
