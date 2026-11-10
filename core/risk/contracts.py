"""Immutable risk and decision contracts. This module never routes orders."""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import math
from typing import Optional, Tuple


class RiskContractError(ValueError):
    pass


class DecisionStatus(str, Enum):
    APPROVED = "APPROVED"
    HOLD = "HOLD"
    REJECTED = "REJECTED"


class RiskSeverity(str, Enum):
    INFO = "INFO"
    LOW = "LOW"
    MODERATE = "MODERATE"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class DecisionReason(str, Enum):
    SIGNAL_NOT_ACCEPTED = "SIGNAL_NOT_ACCEPTED"
    SIGNAL_MISSING = "SIGNAL_MISSING"
    INSTRUMENT_MISMATCH = "INSTRUMENT_MISMATCH"
    INVALID_SIGNAL_PROVENANCE = "INVALID_SIGNAL_PROVENANCE"
    SIGNAL_CLOCK_INVALID = "SIGNAL_CLOCK_INVALID"
    FEED_NOT_HEALTHY = "FEED_NOT_HEALTHY"
    FEED_STALE = "FEED_STALE"
    BOOK_DESYNCHRONIZED = "BOOK_DESYNCHRONIZED"
    HISTORY_INSUFFICIENT = "HISTORY_INSUFFICIENT"
    DIRECTION_NEUTRAL = "DIRECTION_NEUTRAL"
    LIQUIDITY_DATA_UNAVAILABLE = "LIQUIDITY_DATA_UNAVAILABLE"
    EXPOSURE_DATA_UNAVAILABLE = "EXPOSURE_DATA_UNAVAILABLE"
    SPREAD_TOO_WIDE = "SPREAD_TOO_WIDE"
    DEPTH_INSUFFICIENT = "DEPTH_INSUFFICIENT"
    MARKET_RISK_CRITICAL = "MARKET_RISK_CRITICAL"
    VPIN_RISK_CRITICAL = "VPIN_RISK_CRITICAL"
    GEX_CONTEXT_STALE = "GEX_CONTEXT_STALE"
    EXPOSURE_LIMIT_BREACHED = "EXPOSURE_LIMIT_BREACHED"
    DAILY_LOSS_LIMIT_BREACHED = "DAILY_LOSS_LIMIT_BREACHED"
    RISK_SCORE_TOO_HIGH = "RISK_SCORE_TOO_HIGH"
    RISK_INPUT_INVALID = "RISK_INPUT_INVALID"
    POLICY_INSTRUMENT_MISMATCH = "POLICY_INSTRUMENT_MISMATCH"


@dataclass(frozen=True, slots=True)
class RiskCheck:
    name: str
    passed: bool
    severity: RiskSeverity
    reason: Optional[DecisionReason] = None
    metric: Optional[float] = None
    detail: str = ""

    def __post_init__(self):
        if not isinstance(self.name, str) or not self.name.strip():
            raise RiskContractError("check name required")
        if not isinstance(self.passed, bool):
            raise RiskContractError("passed must be bool")
        if not isinstance(self.severity, RiskSeverity):
            raise RiskContractError("severity must be RiskSeverity")
        if self.reason is not None and not isinstance(self.reason, DecisionReason):
            raise RiskContractError("reason must be DecisionReason")
        if self.metric is not None:
            if isinstance(self.metric, bool) or not isinstance(self.metric, (int, float)) or not math.isfinite(self.metric):
                raise RiskContractError("metric must be finite numeric")


@dataclass(frozen=True, slots=True)
class RiskAssessment:
    instrument_id: str
    risk_score: float
    risk_band: RiskSeverity
    checks: Tuple[RiskCheck, ...]
    hard_veto: bool
    reasons: Tuple[DecisionReason, ...]
    policy_version: str
    audit_sequence: int

    def __post_init__(self):
        if not self.instrument_id or not self.policy_version:
            raise RiskContractError("instrument_id and policy_version required")
        if isinstance(self.risk_score, bool) or not isinstance(self.risk_score, (int, float)) or not math.isfinite(self.risk_score) or not 0 <= self.risk_score <= 1:
            raise RiskContractError("risk_score must be finite and in [0,1]")
        if not isinstance(self.risk_band, RiskSeverity) or not isinstance(self.hard_veto, bool):
            raise RiskContractError("invalid assessment state")
        if not isinstance(self.checks, tuple) or any(not isinstance(x, RiskCheck) for x in self.checks):
            raise RiskContractError("checks must be a tuple of RiskCheck")
        if not isinstance(self.reasons, tuple) or any(not isinstance(x, DecisionReason) for x in self.reasons):
            raise RiskContractError("reasons must be a tuple of DecisionReason")
        if not isinstance(self.audit_sequence, int) or isinstance(self.audit_sequence, bool) or self.audit_sequence < 0:
            raise RiskContractError("audit_sequence must be non-negative integer")


@dataclass(frozen=True, slots=True)
class DecisionEvent:
    decision_id: str
    signal_id: str
    venue: str
    instrument_id: str
    status: DecisionStatus
    direction: str
    risk: RiskAssessment
    source_event_ids: Tuple[str, ...]
    exchange_timestamp_ns: Optional[int]
    receive_timestamp_ns: int
    created_at_receive_timestamp_ns: int
    execution_mode: str = "DISABLED"
    tags: Tuple[str, ...] = ()

    def __post_init__(self):
        for name in ("decision_id", "signal_id", "venue", "instrument_id", "direction"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise RiskContractError(f"{name} required")
        if not isinstance(self.status, DecisionStatus) or not isinstance(self.risk, RiskAssessment):
            raise RiskContractError("typed decision status and risk assessment required")
        if not isinstance(self.source_event_ids, tuple) or len(set(self.source_event_ids)) != len(self.source_event_ids):
            raise RiskContractError("source_event_ids must be a tuple of unique IDs")
        if not self.source_event_ids and not (self.signal_id == "UNAVAILABLE" and self.status is DecisionStatus.REJECTED):
            raise RiskContractError("source_event_ids required for signal-backed decisions")
        if any(not isinstance(x, str) or not x.strip() for x in self.source_event_ids):
            raise RiskContractError("source_event_ids must be non-empty strings")
        for name in ("receive_timestamp_ns", "created_at_receive_timestamp_ns"):
            value = getattr(self, name)
            if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                raise RiskContractError(f"{name} must be non-negative integer")
        if self.exchange_timestamp_ns is not None and (not isinstance(self.exchange_timestamp_ns, int) or isinstance(self.exchange_timestamp_ns, bool) or self.exchange_timestamp_ns < 0):
            raise RiskContractError("exchange_timestamp_ns invalid")
        if self.created_at_receive_timestamp_ns < self.receive_timestamp_ns:
            raise RiskContractError("decision creation precedes source receive time")
        if self.execution_mode != "DISABLED":
            raise RiskContractError("execution is locked to DISABLED")
        if self.risk.instrument_id != self.instrument_id:
            raise RiskContractError("risk assessment instrument mismatch")
