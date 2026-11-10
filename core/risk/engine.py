"""Fail-closed risk evaluation. It consumes FusionResult but never mutates it."""
from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Tuple
import math

from core.risk.contracts import (
    DecisionEvent, DecisionReason, DecisionStatus, RiskAssessment,
    RiskCheck, RiskSeverity,
)
from core.risk.policy import RiskPolicy


@dataclass(frozen=True, slots=True)
class RiskContext:
    now_receive_timestamp_ns: int
    feed_status: str
    feed_age_ns: Optional[int]
    book_synchronized: bool
    history_ready: bool
    market_risk: Optional[float]
    liquidity_spread_ticks: Optional[float]
    liquidity_depth_units: Optional[float]
    vpin_risk: Optional[float]
    gex_age_ns: Optional[int]
    exposure_utilization: Optional[float]
    daily_loss_utilization: Optional[float]
    audit_sequence: int
    # Optional informational fields; no clock-domain arithmetic is performed.
    exchange_reference_timestamp_ns: Optional[int] = None

    def __post_init__(self):
        for name in ("now_receive_timestamp_ns", "audit_sequence"):
            v = getattr(self, name)
            if not isinstance(v, int) or isinstance(v, bool) or v < 0:
                raise ValueError(f"{name} must be non-negative integer")
        if not isinstance(self.book_synchronized, bool) or not isinstance(self.history_ready, bool):
            raise ValueError("book_synchronized and history_ready must be bool")
        if self.exchange_reference_timestamp_ns is not None and (
            not isinstance(self.exchange_reference_timestamp_ns, int)
            or isinstance(self.exchange_reference_timestamp_ns, bool)
            or self.exchange_reference_timestamp_ns < 0
        ):
            raise ValueError("exchange reference timestamp invalid")


@dataclass(frozen=True, slots=True)
class RiskResult:
    decision: DecisionEvent
    assessment: RiskAssessment


def _unit(value: Optional[float], name: str) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or not 0 <= value <= 1:
        raise ValueError(f"{name} must be finite in [0,1]")
    return float(value)


class RiskDecisionEngine:
    def __init__(self, policy: RiskPolicy):
        if not isinstance(policy, RiskPolicy):
            raise TypeError("RiskPolicy required")
        self.policy = policy

    def evaluate(self, fusion_result, context: RiskContext) -> RiskResult:
        from core.signals.contracts import ExecutionMode, SignalDirection
        if not isinstance(context, RiskContext):
            raise TypeError("RiskContext required")
        signal = getattr(fusion_result, "signal", None)
        accepted = bool(getattr(fusion_result, "accepted", False))
        checks = []
        hard_reasons = []
        hold_reasons = []
        soft = {"market": 0.0, "liquidity": 0.0, "vpin": 0.0, "gex": 0.0, "exposure": 0.0}

        def add(name, passed, severity, reason=None, metric=None, detail=""):
            checks.append(RiskCheck(name, passed, severity, reason, metric, detail))
            if not passed and reason is not None:
                if severity is RiskSeverity.CRITICAL:
                    hard_reasons.append(reason)
                else:
                    hold_reasons.append(reason)

        if not accepted or signal is None:
            add("fusion_acceptance", False, RiskSeverity.CRITICAL, DecisionReason.SIGNAL_NOT_ACCEPTED)
        else:
            if signal.instrument_id != self.policy.instrument_id:
                add("instrument_identity", False, RiskSeverity.CRITICAL, DecisionReason.INSTRUMENT_MISMATCH)
            else:
                add("instrument_identity", True, RiskSeverity.INFO)
            if signal.execution_mode is not ExecutionMode.DISABLED:
                add("execution_lock", False, RiskSeverity.CRITICAL, DecisionReason.INVALID_SIGNAL_PROVENANCE)
            else:
                add("execution_lock", True, RiskSeverity.INFO)
            prov = signal.provenance
            if not prov.source_event_ids or len(set(prov.source_event_ids)) != len(prov.source_event_ids):
                add("provenance", False, RiskSeverity.CRITICAL, DecisionReason.INVALID_SIGNAL_PROVENANCE)
            else:
                add("provenance", True, RiskSeverity.INFO)
            if prov.exchange_timestamp_ns is None:
                add("exchange_clock", False, RiskSeverity.CRITICAL, DecisionReason.SIGNAL_CLOCK_INVALID)
            else:
                add("exchange_clock", True, RiskSeverity.INFO)
            if context.now_receive_timestamp_ns < signal.created_at_receive_timestamp_ns:
                add("receive_clock_order", False, RiskSeverity.CRITICAL, DecisionReason.SIGNAL_CLOCK_INVALID)
            else:
                age = context.now_receive_timestamp_ns - signal.created_at_receive_timestamp_ns
                if age > self.policy.max_signal_age_ns:
                    add("signal_freshness", False, RiskSeverity.CRITICAL, DecisionReason.FEED_STALE, float(age))
                else:
                    add("signal_freshness", True, RiskSeverity.INFO, metric=float(age))
            if signal.direction is SignalDirection.NEUTRAL:
                add("direction", False, RiskSeverity.MODERATE, DecisionReason.DIRECTION_NEUTRAL)
            else:
                add("direction", True, RiskSeverity.INFO)

        if not isinstance(context.feed_status, str) or not context.feed_status.strip():
            add("feed_status", False, RiskSeverity.CRITICAL, DecisionReason.FEED_NOT_HEALTHY)
        elif context.feed_status.upper() != "HEALTHY":
            reason = DecisionReason.FEED_STALE if context.feed_status.upper() == "STALE" else DecisionReason.FEED_NOT_HEALTHY
            add("feed_status", False, RiskSeverity.CRITICAL, reason)
        else:
            add("feed_status", True, RiskSeverity.INFO)
        if context.feed_age_ns is None:
            add("feed_age", False, RiskSeverity.CRITICAL, DecisionReason.FEED_STALE)
        elif isinstance(context.feed_age_ns, bool) or not isinstance(context.feed_age_ns, int) or context.feed_age_ns < 0:
            add("feed_age", False, RiskSeverity.CRITICAL, DecisionReason.RISK_INPUT_INVALID)
        elif context.feed_age_ns > self.policy.max_feed_age_ns:
            add("feed_age", False, RiskSeverity.CRITICAL, DecisionReason.FEED_STALE, float(context.feed_age_ns))
        else:
            add("feed_age", True, RiskSeverity.INFO, metric=float(context.feed_age_ns))
        if not context.book_synchronized:
            add("book_sync", False, RiskSeverity.CRITICAL, DecisionReason.BOOK_DESYNCHRONIZED)
        else:
            add("book_sync", True, RiskSeverity.INFO)
        if not context.history_ready:
            add("history", False, RiskSeverity.CRITICAL, DecisionReason.HISTORY_INSUFFICIENT)
        else:
            add("history", True, RiskSeverity.INFO)

        market = _unit(context.market_risk, "market_risk")
        if market is None:
            hold_reasons.append(DecisionReason.RISK_INPUT_INVALID)
            checks.append(RiskCheck("market_risk", False, RiskSeverity.MODERATE, DecisionReason.RISK_INPUT_INVALID, detail="market risk unavailable"))
            soft["market"] = 1.0
        else:
            soft["market"] = market
            add("market_risk", True, RiskSeverity.INFO, metric=market)
            if market >= 0.95:
                add("market_critical", False, RiskSeverity.CRITICAL, DecisionReason.MARKET_RISK_CRITICAL, market)

        spread = context.liquidity_spread_ticks
        depth = context.liquidity_depth_units
        if spread is None or depth is None:
            hold_reasons.append(DecisionReason.LIQUIDITY_DATA_UNAVAILABLE)
            checks.append(RiskCheck("liquidity", False, RiskSeverity.MODERATE, DecisionReason.LIQUIDITY_DATA_UNAVAILABLE, detail="spread/depth unavailable"))
            soft["liquidity"] = 1.0
        else:
            if any(isinstance(x, bool) or not isinstance(x, (int, float)) or not math.isfinite(x) or x < 0 for x in (spread, depth)):
                raise ValueError("liquidity metrics must be finite and non-negative")
            spread_risk = min(1.0, float(spread) / self.policy.max_spread_ticks)
            depth_risk = max(0.0, 1.0 - float(depth) / self.policy.min_depth_units)
            soft["liquidity"] = max(spread_risk, depth_risk)
            if spread > self.policy.max_spread_ticks:
                add("spread", False, RiskSeverity.MODERATE, DecisionReason.SPREAD_TOO_WIDE, float(spread))
            else:
                add("spread", True, RiskSeverity.INFO, metric=float(spread))
            if depth < self.policy.min_depth_units:
                add("depth", False, RiskSeverity.MODERATE, DecisionReason.DEPTH_INSUFFICIENT, float(depth))
            else:
                add("depth", True, RiskSeverity.INFO, metric=float(depth))

        vpin = _unit(context.vpin_risk, "vpin_risk")
        if vpin is None:
            hold_reasons.append(DecisionReason.RISK_INPUT_INVALID)
            checks.append(RiskCheck("vpin_risk", False, RiskSeverity.MODERATE, DecisionReason.RISK_INPUT_INVALID, detail="VPIN risk unavailable"))
            soft["vpin"] = 1.0
        else:
            soft["vpin"] = vpin
            add("vpin_risk", True, RiskSeverity.INFO, metric=vpin)
            if vpin >= self.policy.critical_vpin:
                add("vpin_critical", False, RiskSeverity.CRITICAL, DecisionReason.VPIN_RISK_CRITICAL, vpin)

        if context.gex_age_ns is None:
            hold_reasons.append(DecisionReason.GEX_CONTEXT_STALE)
            checks.append(RiskCheck("gex_freshness", False, RiskSeverity.MODERATE, DecisionReason.GEX_CONTEXT_STALE, detail="GEX age unavailable"))
            soft["gex"] = 1.0
        elif isinstance(context.gex_age_ns, bool) or not isinstance(context.gex_age_ns, int) or context.gex_age_ns < 0:
            raise ValueError("gex_age_ns must be non-negative integer")
        else:
            gex_risk = min(1.0, context.gex_age_ns / self.policy.max_gex_age_ns)
            soft["gex"] = gex_risk
            if context.gex_age_ns > self.policy.max_gex_age_ns:
                add("gex_freshness", False, RiskSeverity.MODERATE, DecisionReason.GEX_CONTEXT_STALE, float(context.gex_age_ns))
            else:
                add("gex_freshness", True, RiskSeverity.INFO, metric=float(context.gex_age_ns))

        exposure = _unit(context.exposure_utilization, "exposure_utilization")
        daily_loss = _unit(context.daily_loss_utilization, "daily_loss_utilization")
        if exposure is None or daily_loss is None:
            hold_reasons.append(DecisionReason.EXPOSURE_DATA_UNAVAILABLE)
            checks.append(RiskCheck("exposure", False, RiskSeverity.MODERATE, DecisionReason.EXPOSURE_DATA_UNAVAILABLE, detail="account exposure/loss data unavailable"))
            soft["exposure"] = 1.0
        else:
            soft["exposure"] = max(exposure, daily_loss)
            if exposure > self.policy.max_exposure_utilization:
                add("exposure_limit", False, RiskSeverity.CRITICAL, DecisionReason.EXPOSURE_LIMIT_BREACHED, exposure)
            else:
                add("exposure_limit", True, RiskSeverity.INFO, metric=exposure)
            if daily_loss > self.policy.max_daily_loss_utilization:
                add("daily_loss_limit", False, RiskSeverity.CRITICAL, DecisionReason.DAILY_LOSS_LIMIT_BREACHED, daily_loss)
            else:
                add("daily_loss_limit", True, RiskSeverity.INFO, metric=daily_loss)

        weights = dict(self.policy.weights)
        risk_score = min(1.0, sum(weights[k] * soft[k] for k in weights))
        soft_score_reject = risk_score >= self.policy.reject_risk_score
        unique_hard = tuple(dict.fromkeys(hard_reasons))
        unique_hold = tuple(dict.fromkeys(hold_reasons))
        reasons = unique_hard or unique_hold
        hard_veto = bool(unique_hard)
        if hard_veto or soft_score_reject:
            status = DecisionStatus.REJECTED
            if soft_score_reject and not hard_veto:
                reasons = (DecisionReason.RISK_SCORE_TOO_HIGH,)
        elif unique_hold or risk_score >= self.policy.hold_risk_score:
            status = DecisionStatus.HOLD
            if risk_score >= self.policy.hold_risk_score and not unique_hold:
                reasons = (DecisionReason.RISK_SCORE_TOO_HIGH,)
        else:
            status = DecisionStatus.APPROVED
            reasons = ()

        if risk_score < 0.30:
            band = RiskSeverity.LOW
        elif risk_score < 0.60:
            band = RiskSeverity.MODERATE
        elif risk_score < 0.80:
            band = RiskSeverity.HIGH
        else:
            band = RiskSeverity.CRITICAL

        assessment = RiskAssessment(
            self.policy.instrument_id, risk_score, band, tuple(checks),
            hard_veto, tuple(reasons), self.policy.policy_version, context.audit_sequence,
        )
        if signal is None:
            # Rejected decisions still carry explicit identity and empty-but-real provenance is prohibited.
            # Use stable sentinel identity only for the diagnostic record; no source provenance is fabricated.
            signal_id = "UNAVAILABLE"
            venue = "UNKNOWN"
            direction = "NEUTRAL"
            source_ids = ()
            exchange_ts = None
            receive_ts = context.now_receive_timestamp_ns
        else:
            signal_id = signal.signal_id
            venue = signal.venue
            direction = signal.direction.value
            source_ids = signal.provenance.source_event_ids
            exchange_ts = signal.provenance.exchange_timestamp_ns
            receive_ts = signal.provenance.receive_timestamp_ns
        decision = DecisionEvent(
            decision_id=f"risk:{venue}:{self.policy.instrument_id}:{context.audit_sequence}",
            signal_id=signal_id, venue=venue, instrument_id=self.policy.instrument_id,
            status=status, direction=direction, risk=assessment,
            source_event_ids=source_ids, exchange_timestamp_ns=exchange_ts,
            receive_timestamp_ns=receive_ts,
            created_at_receive_timestamp_ns=max(context.now_receive_timestamp_ns, receive_ts),
            execution_mode="DISABLED",
            tags=("RISK_GATE_V1", "EXECUTION_DISABLED"),
        )
        return RiskResult(decision, assessment)
