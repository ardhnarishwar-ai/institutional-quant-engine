"""Deterministic signal eligibility gates over feed, book and history quality."""
from __future__ import annotations

from dataclasses import dataclass
from typing import FrozenSet, Optional, Tuple

from core.signals.contracts import RejectionReason


@dataclass(frozen=True, slots=True)
class QualityGatePolicy:
    max_feed_age_ns: int = 2_000_000_000
    min_history_events: int = 100
    min_classified_trades: int = 20
    require_book_sync: bool = True
    allowed_instruments: Optional[FrozenSet[str]] = None
    require_exchange_timestamp: bool = True

    def __post_init__(self):
        for name in ("max_feed_age_ns", "min_history_events", "min_classified_trades"):
            value = getattr(self, name)
            if not isinstance(value, int) or isinstance(value, bool) or value < (1 if name == "max_feed_age_ns" else 0):
                raise ValueError(f"{name} has an invalid value")
        if self.allowed_instruments is not None:
            if not isinstance(self.allowed_instruments, frozenset) or any(not isinstance(x, str) or not x.strip() for x in self.allowed_instruments):
                raise ValueError("allowed_instruments must be None or frozenset of canonical instrument IDs")


@dataclass(frozen=True, slots=True)
class QualityGateInput:
    venue: str
    instrument_id: str
    feed_status: str
    feed_age_ns: Optional[int]
    book_synchronized: bool
    history_event_count: int
    classified_trade_count: int
    exchange_timestamp_ns: Optional[int]
    receive_timestamp_ns: int
    source_event_ids: Tuple[str, ...]
    analytics_audit_sequence: int

    def __post_init__(self):
        if not self.venue or not self.instrument_id:
            raise ValueError("venue and instrument_id are required")
        if not isinstance(self.feed_status, str):
            raise ValueError("feed_status must be a string")
        if self.feed_age_ns is not None and (not isinstance(self.feed_age_ns, int) or isinstance(self.feed_age_ns, bool) or self.feed_age_ns < 0):
            raise ValueError("feed_age_ns must be None or non-negative integer")
        if not isinstance(self.book_synchronized, bool):
            raise ValueError("book_synchronized must be bool")
        for name in ("history_event_count", "classified_trade_count", "receive_timestamp_ns", "analytics_audit_sequence"):
            value = getattr(self, name)
            if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                raise ValueError(f"{name} must be a non-negative integer")
        if self.exchange_timestamp_ns is not None and (not isinstance(self.exchange_timestamp_ns, int) or isinstance(self.exchange_timestamp_ns, bool) or self.exchange_timestamp_ns < 0):
            raise ValueError("exchange_timestamp_ns must be None or non-negative integer")
        if not isinstance(self.source_event_ids, tuple):
            raise ValueError("source_event_ids must be a tuple")


@dataclass(frozen=True, slots=True)
class QualityGateResult:
    eligible: bool
    rejection_reasons: Tuple[RejectionReason, ...]
    venue: str
    instrument_id: str
    feed_age_ns: Optional[int]
    history_event_count: int
    classified_trade_count: int

    def __post_init__(self):
        if self.eligible != (len(self.rejection_reasons) == 0):
            raise ValueError("eligible must exactly reflect absence of rejection reasons")


class SignalQualityGate:
    def __init__(self, policy: QualityGatePolicy = QualityGatePolicy()):
        self.policy = policy

    def evaluate(self, item: QualityGateInput) -> QualityGateResult:
        reasons = []
        status = item.feed_status.upper()
        if status == "STALE":
            reasons.append(RejectionReason.FEED_STALE)
        elif status == "DESYNCHRONIZED":
            reasons.append(RejectionReason.FEED_DESYNCHRONIZED)
        elif status == "DISCONNECTED":
            reasons.append(RejectionReason.FEED_DISCONNECTED)
        elif status != "HEALTHY":
            reasons.append(RejectionReason.FEED_NOT_HEALTHY)

        if item.feed_age_ns is None or item.feed_age_ns > self.policy.max_feed_age_ns:
            if RejectionReason.FEED_STALE not in reasons:
                reasons.append(RejectionReason.FEED_STALE)
        if self.policy.require_book_sync and not item.book_synchronized:
            reasons.append(RejectionReason.BOOK_NOT_SYNCHRONIZED)
        if item.history_event_count < self.policy.min_history_events:
            reasons.append(RejectionReason.INSUFFICIENT_HISTORY)
        if item.classified_trade_count < self.policy.min_classified_trades:
            reasons.append(RejectionReason.INSUFFICIENT_CLASSIFIED_TRADES)
        if self.policy.allowed_instruments is not None and item.instrument_id not in self.policy.allowed_instruments:
            reasons.append(RejectionReason.INSTRUMENT_NOT_ALLOWED)
        if self.policy.require_exchange_timestamp and item.exchange_timestamp_ns is None:
            reasons.append(RejectionReason.MISSING_EXCHANGE_TIMESTAMP)
        if not item.source_event_ids:
            reasons.append(RejectionReason.MISSING_PROVENANCE)
        if item.exchange_timestamp_ns is not None and item.exchange_timestamp_ns > item.receive_timestamp_ns:
            reasons.append(RejectionReason.INVALID_CLOCK_ORDER)

        # Stable taxonomy ordering and no duplicate reason values.
        unique = tuple(dict.fromkeys(reasons))
        return QualityGateResult(
            eligible=not unique, rejection_reasons=unique,
            venue=item.venue.upper(), instrument_id=item.instrument_id,
            feed_age_ns=item.feed_age_ns,
            history_event_count=item.history_event_count,
            classified_trade_count=item.classified_trade_count,
        )
