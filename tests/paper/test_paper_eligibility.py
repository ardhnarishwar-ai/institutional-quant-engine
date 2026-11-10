from dataclasses import replace
from types import SimpleNamespace
from core.paper.eligibility import EligibilityReason, PaperEligibilityGate
from core.risk.contracts import DecisionEvent, DecisionStatus, RiskAssessment, RiskSeverity


def approved_decision(**overrides):
    risk = RiskAssessment(
        instrument_id="NSE:NSE_SPOT:NIFTY:INR",
        risk_score=0.1,
        risk_band=RiskSeverity.LOW,
        checks=(),
        hard_veto=False,
        reasons=(),
        policy_version="risk-v1",
        audit_sequence=1,
    )
    values = dict(
        decision_id="d-1", signal_id="s-1", venue="NSE",
        instrument_id="NSE:NSE_SPOT:NIFTY:INR",
        status=DecisionStatus.APPROVED, direction="LONG",
        risk=risk, source_event_ids=("journal-event-1",), exchange_timestamp_ns=None,
        receive_timestamp_ns=100, created_at_receive_timestamp_ns=101,
        execution_mode="DISABLED",
    )
    values.update(overrides)
    return DecisionEvent(**values)


def test_approved_decision_is_eligible_without_enabling_execution():
    gate = PaperEligibilityGate()
    result = gate.evaluate(approved_decision())
    assert result.eligible
    assert result.reason is EligibilityReason.ELIGIBLE


def test_hold_and_rejected_decisions_never_become_eligible():
    for status in (DecisionStatus.HOLD, DecisionStatus.REJECTED):
        result = PaperEligibilityGate().evaluate(approved_decision(status=status))
        assert not result.eligible
        assert result.reason is EligibilityReason.DECISION_NOT_APPROVED


def test_duplicate_approved_decision_is_rejected_idempotently():
    gate = PaperEligibilityGate()
    decision = approved_decision()
    assert gate.evaluate(decision).eligible
    result = gate.evaluate(decision)
    assert not result.eligible
    assert result.reason is EligibilityReason.DUPLICATE_DECISION


def test_instrument_and_venue_isolation():
    gate = PaperEligibilityGate()
    decision = approved_decision()
    assert gate.evaluate(decision, expected_instrument_id="BTCUSDT").reason is EligibilityReason.INSTRUMENT_MISMATCH
    assert gate.evaluate(decision, expected_venue="BINANCE").reason is EligibilityReason.VENUE_MISMATCH
    assert gate.evaluate(decision, expected_instrument_id=decision.instrument_id, expected_venue="NSE").eligible


def proxy(decision, **changes):
    values = {name: getattr(decision, name) for name in (
        "decision_id", "signal_id", "venue", "instrument_id", "status",
        "direction", "risk", "source_event_ids", "exchange_timestamp_ns",
        "receive_timestamp_ns", "created_at_receive_timestamp_ns",
        "execution_mode",
    )}
    values.update(changes)
    return SimpleNamespace(**values)


def test_missing_or_true_hard_veto_fails_closed():
    gate = PaperEligibilityGate()
    decision = approved_decision()
    assert gate.evaluate(proxy(decision, risk=None)).reason is EligibilityReason.MISSING_RISK_ASSESSMENT
    risk = replace(decision.risk, hard_veto=True)
    assert gate.evaluate(replace(decision, risk=risk)).reason is EligibilityReason.HARD_VETO_PRESENT


def test_missing_provenance_and_invalid_clock_fail_closed():
    gate = PaperEligibilityGate()
    decision = approved_decision()
    assert gate.evaluate(proxy(decision, source_event_ids=())).reason is EligibilityReason.MISSING_PROVENANCE
    assert gate.evaluate(proxy(decision, created_at_receive_timestamp_ns=None)).reason is EligibilityReason.INVALID_DECISION_CLOCK


def test_non_disabled_execution_mode_is_rejected():
    result = PaperEligibilityGate().evaluate(proxy(approved_decision(), execution_mode="LIVE"))
    assert result.reason is EligibilityReason.EXECUTION_MODE_NOT_DISABLED
