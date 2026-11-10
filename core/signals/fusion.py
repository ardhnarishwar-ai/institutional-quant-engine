from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Tuple
from core.signals.contracts import ConfidenceMetrics,ExecutionMode,RejectionReason,SignalDirection,SignalEvent,SignalProvenance
from core.signals.evidence import ChannelEvidence,EvidenceBundle
from core.signals.fusion_config import FusionPolicy
from core.signals.quality_gates import QualityGateInput,QualityGatePolicy,SignalQualityGate

@dataclass(frozen=True, slots=True)
class FusionContext:
    feed_status: str
    feed_age_ns: Optional[int]
    book_synchronized: bool
    gex_as_of_timestamp_ns: Optional[int] = None

@dataclass(frozen=True, slots=True)
class FusionResult:
    accepted: bool
    direction: Optional[SignalDirection]
    signal: Optional[SignalEvent]
    score: float
    evidence_coverage: float
    directional_votes: int
    rejection_reasons: Tuple[RejectionReason,...]
    evidence: Tuple[ChannelEvidence,...]
    tags: Tuple[str,...]

class MicrostructureFusionEngine:
    def __init__(self,policy:FusionPolicy):
        self.policy=policy
        self.gate=SignalQualityGate(QualityGatePolicy(max_feed_age_ns=policy.max_feed_age_ns,min_history_events=policy.minimum_history_events,min_classified_trades=policy.minimum_classified_trades,require_book_sync=True,allowed_instruments=frozenset({policy.instrument_id}),require_exchange_timestamp=True))
    @staticmethod
    def _reject(reasons,evidence=()):
        return FusionResult(False,None,None,0.0,0.0,0,tuple(dict.fromkeys(reasons)),tuple(evidence),("QUALITY_REJECTED",))
    def evaluate(self,bundle:EvidenceBundle,context:FusionContext)->FusionResult:
        if not isinstance(bundle,EvidenceBundle) or not isinstance(context,FusionContext): raise TypeError("typed EvidenceBundle and FusionContext required")
        channels=(bundle.cvd,bundle.volume_profile,bundle.vpin,bundle.absorption,bundle.gex)
        if bundle.instrument_id!=self.policy.instrument_id: return self._reject((RejectionReason.INSTRUMENT_NOT_ALLOWED,),channels)
        gate=self.gate.evaluate(QualityGateInput(bundle.venue,bundle.instrument_id,context.feed_status,context.feed_age_ns,context.book_synchronized,bundle.history_event_count,bundle.classified_trade_count,bundle.exchange_timestamp_ns,bundle.receive_timestamp_ns,bundle.source_event_ids,bundle.audit_sequence))
        if not gate.eligible: return self._reject(gate.rejection_reasons,channels)
        if context.gex_as_of_timestamp_ns is not None:
            if bundle.exchange_timestamp_ns is None or context.gex_as_of_timestamp_ns>bundle.exchange_timestamp_ns: return self._reject((RejectionReason.INVALID_CLOCK_ORDER,),channels)
        directional={"cvd":bundle.cvd,"volume_profile":bundle.volume_profile,"absorption":bundle.absorption}
        weights=dict(self.policy.weights); available=sum(weights[k] for k,v in directional.items() if v.available)
        coverage=available/sum(weights.values())
        if coverage<self.policy.minimum_coverage: return self._reject((RejectionReason.INSUFFICIENT_HISTORY,),channels)
        total=0.0; pos=[]; neg=[]
        for key,item in directional.items():
            if item.available and item.directional_value is not None:
                value=float(item.directional_value); total+=weights[key]*value
                if value>0: pos.append(value)
                elif value<0: neg.append(abs(value))
        score=total/available if available else 0.0
        if pos and neg and min(max(pos),max(neg))>=self.policy.conflict_tolerance:
            direction=SignalDirection.NEUTRAL
        elif score>=self.policy.minimum_absolute_score and len(pos)>=self.policy.minimum_directional_votes:
            direction=SignalDirection.LONG
        elif score<=-self.policy.minimum_absolute_score and len(neg)>=self.policy.minimum_directional_votes:
            direction=SignalDirection.SHORT
        else: direction=SignalDirection.NEUTRAL
        penalty=min(self.policy.max_vpin_risk_penalty,max(0.0,float(bundle.vpin.risk_modifier))) if bundle.vpin.available else 0.0
        confidence=max(0.0,min(1.0,abs(score)*coverage*(1.0-penalty)))
        if direction is SignalDirection.NEUTRAL: confidence=min(confidence,self.policy.minimum_absolute_score)
        tags=["FUSION_V1","EXECUTION_DISABLED"]
        if pos and neg: tags.append("DIRECTIONAL_CONFLICT_NEUTRALIZED")
        if bundle.vpin.available and bundle.vpin.risk_modifier>0: tags.append("VPIN_TOXICITY_ELEVATED")
        if bundle.gex.available and context.gex_as_of_timestamp_ns is not None and bundle.exchange_timestamp_ns-context.gex_as_of_timestamp_ns<=self.policy.max_gex_age_ns: tags.append("GEX_CONTEXT_PRESENT")
        signal=SignalEvent(signal_id=f"fusion:{bundle.venue}:{bundle.instrument_id}:{bundle.audit_sequence}",venue=bundle.venue,instrument_id=bundle.instrument_id,direction=direction,confidence=ConfidenceMetrics(confidence,coverage,len(bundle.source_event_ids),"UNCALIBRATED"),provenance=SignalProvenance(bundle.source_event_ids,bundle.exchange_timestamp_ns,bundle.receive_timestamp_ns,bundle.audit_sequence),created_at_receive_timestamp_ns=bundle.receive_timestamp_ns,execution_mode=ExecutionMode.DISABLED,tags=tuple(tags))
        return FusionResult(True,direction,signal,score,coverage,len(pos)+len(neg),(),channels,tuple(tags))
