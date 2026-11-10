"""Fail-closed eligibility gate for paper-only order creation."""

from dataclasses import dataclass
from enum import Enum
from typing import Optional, Set


class EligibilityReason(str, Enum):
    ELIGIBLE = "ELIGIBLE"
    DECISION_NOT_APPROVED = "DECISION_NOT_APPROVED"
    EXECUTION_MODE_NOT_DISABLED = "EXECUTION_MODE_NOT_DISABLED"
    HARD_VETO_PRESENT = "HARD_VETO_PRESENT"
    MISSING_RISK_ASSESSMENT = "MISSING_RISK_ASSESSMENT"
    MISSING_PROVENANCE = "MISSING_PROVENANCE"
    INVALID_DECISION_CLOCK = "INVALID_DECISION_CLOCK"
    INSTRUMENT_MISMATCH = "INSTRUMENT_MISMATCH"
    VENUE_MISMATCH = "VENUE_MISMATCH"
    DUPLICATE_DECISION = "DUPLICATE_DECISION"
    INVALID_DECISION_ID = "INVALID_DECISION_ID"


@dataclass(frozen=True, slots=True)
class EligibilityResult:
    eligible: bool
    reason: EligibilityReason
    decision_id: Optional[str]
    instrument_id: Optional[str]
    audit_sequence: Optional[int]


class PaperEligibilityGate:
    """Stateful idempotency gate. It never submits or routes broker orders."""

    def __init__(self) -> None:
        self._seen_decision_ids: Set[str] = set()

    def evaluate(
        self,
        decision: object,
        *,
        expected_instrument_id: Optional[str] = None,
        expected_venue: Optional[str] = None,
    ) -> EligibilityResult:
        decision_id = getattr(decision, "decision_id", None)
        instrument_id = getattr(decision, "instrument_id", None)
        venue = getattr(decision, "venue", None)
        audit_sequence = getattr(decision, "audit_sequence", getattr(getattr(decision, "risk", None), "audit_sequence", None))

        def result(ok: bool, reason: EligibilityReason) -> EligibilityResult:
            return EligibilityResult(ok, reason, decision_id, instrument_id, audit_sequence)

        if not isinstance(decision_id, str) or not decision_id.strip():
            return result(False, EligibilityReason.INVALID_DECISION_ID)
        if decision_id in self._seen_decision_ids:
            return result(False, EligibilityReason.DUPLICATE_DECISION)
        if getattr(getattr(decision, "status", None), "value", getattr(decision, "status", None)) != "APPROVED":
            return result(False, EligibilityReason.DECISION_NOT_APPROVED)
        if getattr(getattr(decision, "execution_mode", None), "value", getattr(decision, "execution_mode", None)) != "DISABLED":
            return result(False, EligibilityReason.EXECUTION_MODE_NOT_DISABLED)

        risk = getattr(decision, "risk", None)
        if risk is None:
            return result(False, EligibilityReason.MISSING_RISK_ASSESSMENT)
        if getattr(risk, "hard_veto", None) is not False:
            return result(False, EligibilityReason.HARD_VETO_PRESENT)

        source_ids = getattr(decision, "source_event_ids", None)
        if not isinstance(source_ids, (tuple, list)) or not source_ids or any(not isinstance(x, str) or not x.strip() for x in source_ids):
            return result(False, EligibilityReason.MISSING_PROVENANCE)

        decision_ts = getattr(decision, "created_at_receive_timestamp_ns", None)
        receive_ts = getattr(decision, "receive_timestamp_ns", None)
        if (
            isinstance(decision_ts, bool) or not isinstance(decision_ts, int) or decision_ts < 0
            or isinstance(receive_ts, bool) or not isinstance(receive_ts, int) or receive_ts < 0
        ):
            return result(False, EligibilityReason.INVALID_DECISION_CLOCK)

        if not isinstance(instrument_id, str) or not instrument_id.strip():
            return result(False, EligibilityReason.INSTRUMENT_MISMATCH)
        if expected_instrument_id is not None and instrument_id != expected_instrument_id:
            return result(False, EligibilityReason.INSTRUMENT_MISMATCH)
        if expected_venue is not None and venue != expected_venue:
            return result(False, EligibilityReason.VENUE_MISMATCH)

        # Only eligible decisions are consumed by the idempotency set. HOLD,
        # REJECTED and malformed decisions can be re-evaluated after correction.
        self._seen_decision_ids.add(decision_id)
        return result(True, EligibilityReason.ELIGIBLE)
