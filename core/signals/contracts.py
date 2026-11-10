"""Immutable, auditable, venue-agnostic signal contracts.

This layer describes a decision; it never places, modifies, or routes orders.
Confidence is an evidence score, not a calibrated probability of profit.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import math
from typing import Optional, Tuple


class SignalContractError(ValueError):
    pass


class SignalDirection(str, Enum):
    LONG = "LONG"
    SHORT = "SHORT"
    NEUTRAL = "NEUTRAL"


class ExecutionMode(str, Enum):
    DISABLED = "DISABLED"


class RejectionReason(str, Enum):
    FEED_NOT_HEALTHY = "FEED_NOT_HEALTHY"
    FEED_STALE = "FEED_STALE"
    FEED_DESYNCHRONIZED = "FEED_DESYNCHRONIZED"
    FEED_DISCONNECTED = "FEED_DISCONNECTED"
    BOOK_NOT_SYNCHRONIZED = "BOOK_NOT_SYNCHRONIZED"
    INSUFFICIENT_HISTORY = "INSUFFICIENT_HISTORY"
    INSUFFICIENT_CLASSIFIED_TRADES = "INSUFFICIENT_CLASSIFIED_TRADES"
    INSTRUMENT_NOT_ALLOWED = "INSTRUMENT_NOT_ALLOWED"
    MISSING_EXCHANGE_TIMESTAMP = "MISSING_EXCHANGE_TIMESTAMP"
    MISSING_PROVENANCE = "MISSING_PROVENANCE"
    INVALID_CLOCK_ORDER = "INVALID_CLOCK_ORDER"


def _nonempty(value: str, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise SignalContractError(f"{name} is required")
    return value.strip()


def _timestamp(value: Optional[int], name: str, *, optional: bool = False):
    if value is None and optional:
        return
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise SignalContractError(f"{name} must be a non-negative integer")


@dataclass(frozen=True, slots=True)
class ConfidenceMetrics:
    """Evidence strength only; calibration is explicitly tracked separately."""
    score: float
    evidence_coverage: float
    sample_size: int
    calibration_label: str = "UNCALIBRATED"

    def __post_init__(self):
        for name in ("score", "evidence_coverage"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
                raise SignalContractError(f"{name} must be finite numeric")
            if not 0.0 <= value <= 1.0:
                raise SignalContractError(f"{name} must be in [0, 1]")
        if not isinstance(self.sample_size, int) or isinstance(self.sample_size, bool) or self.sample_size < 0:
            raise SignalContractError("sample_size must be a non-negative integer")
        _nonempty(self.calibration_label, "calibration_label")


@dataclass(frozen=True, slots=True)
class SignalProvenance:
    source_event_ids: Tuple[str, ...]
    exchange_timestamp_ns: Optional[int]
    receive_timestamp_ns: int
    analytics_audit_sequence: int

    def __post_init__(self):
        if not isinstance(self.source_event_ids, tuple) or not self.source_event_ids:
            raise SignalContractError("source_event_ids must be a non-empty tuple")
        if any(not isinstance(x, str) or not x.strip() for x in self.source_event_ids):
            raise SignalContractError("source_event_ids must contain non-empty strings")
        if len(set(self.source_event_ids)) != len(self.source_event_ids):
            raise SignalContractError("source_event_ids must be unique")
        _timestamp(self.exchange_timestamp_ns, "exchange_timestamp_ns", optional=True)
        _timestamp(self.receive_timestamp_ns, "receive_timestamp_ns")
        if not isinstance(self.analytics_audit_sequence, int) or isinstance(self.analytics_audit_sequence, bool) or self.analytics_audit_sequence < 0:
            raise SignalContractError("analytics_audit_sequence must be a non-negative integer")


@dataclass(frozen=True, slots=True)
class SignalEvent:
    signal_id: str
    venue: str
    instrument_id: str
    direction: SignalDirection
    confidence: ConfidenceMetrics
    provenance: SignalProvenance
    created_at_receive_timestamp_ns: int
    execution_mode: ExecutionMode = ExecutionMode.DISABLED
    tags: Tuple[str, ...] = ()

    def __post_init__(self):
        object.__setattr__(self, "signal_id", _nonempty(self.signal_id, "signal_id"))
        object.__setattr__(self, "venue", _nonempty(self.venue, "venue").upper())
        object.__setattr__(self, "instrument_id", _nonempty(self.instrument_id, "instrument_id"))
        if not isinstance(self.direction, SignalDirection):
            raise SignalContractError("direction must be SignalDirection")
        if not isinstance(self.confidence, ConfidenceMetrics):
            raise SignalContractError("confidence must be ConfidenceMetrics")
        if not isinstance(self.provenance, SignalProvenance):
            raise SignalContractError("provenance must be SignalProvenance")
        _timestamp(self.created_at_receive_timestamp_ns, "created_at_receive_timestamp_ns")
        if self.created_at_receive_timestamp_ns < self.provenance.receive_timestamp_ns:
            raise SignalContractError("signal creation time cannot precede source receive time")
        if self.execution_mode is not ExecutionMode.DISABLED:
            raise SignalContractError("execution is strictly disabled in Stage 5B.1")
        if not isinstance(self.tags, tuple) or any(not isinstance(x, str) or not x.strip() for x in self.tags):
            raise SignalContractError("tags must be a tuple of non-empty strings")
