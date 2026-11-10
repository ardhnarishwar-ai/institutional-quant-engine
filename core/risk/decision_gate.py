"""Thin decision-gate facade; upstream fusion objects remain untouched."""
from core.risk.engine import RiskContext, RiskDecisionEngine, RiskResult
from core.risk.policy import RiskPolicy


class DecisionGate:
    def __init__(self, policy: RiskPolicy):
        self.engine = RiskDecisionEngine(policy)

    def evaluate(self, fusion_result, context: RiskContext) -> RiskResult:
        return self.engine.evaluate(fusion_result, context)
