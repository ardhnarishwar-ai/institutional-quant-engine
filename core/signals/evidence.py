from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Tuple

@dataclass(frozen=True, slots=True)
class ChannelEvidence:
    channel: str
    available: bool
    directional_value: Optional[float]
    reason: Optional[str] = None
    risk_modifier: float = 0.0
    context: Tuple[Tuple[str, str], ...] = ()
    def __post_init__(self):
        if not isinstance(self.channel, str) or not self.channel.strip(): raise ValueError("channel required")
        if not isinstance(self.available, bool): raise ValueError("available must be bool")
        if self.directional_value is not None:
            if isinstance(self.directional_value, bool) or not isinstance(self.directional_value, (int,float)) or not -1 <= self.directional_value <= 1: raise ValueError("directional_value must be in [-1,1]")
        if isinstance(self.risk_modifier, bool) or not isinstance(self.risk_modifier,(int,float)) or not -1 <= self.risk_modifier <= 1: raise ValueError("risk_modifier must be in [-1,1]")
        if not self.available and self.directional_value is not None: raise ValueError("unavailable evidence cannot vote")
        if not isinstance(self.context, tuple): raise ValueError("context must be tuple")

@dataclass(frozen=True, slots=True)
class EvidenceBundle:
    venue: str
    instrument_id: str
    session_id: str
    audit_sequence: int
    exchange_timestamp_ns: Optional[int]
    receive_timestamp_ns: int
    source_event_ids: Tuple[str, ...]
    cvd: ChannelEvidence
    volume_profile: ChannelEvidence
    vpin: ChannelEvidence
    absorption: ChannelEvidence
    gex: ChannelEvidence
    history_event_count: int
    classified_trade_count: int
    def __post_init__(self):
        if not self.venue or not self.instrument_id or not self.session_id: raise ValueError("identity fields required")
        if not isinstance(self.source_event_ids,tuple) or not self.source_event_ids or any(not isinstance(x,str) or not x.strip() for x in self.source_event_ids): raise ValueError("genuine journal source_event_ids required")
        if len(set(self.source_event_ids)) != len(self.source_event_ids): raise ValueError("source_event_ids must be unique")
        for name in ("audit_sequence","receive_timestamp_ns","history_event_count","classified_trade_count"):
            v=getattr(self,name)
            if not isinstance(v,int) or isinstance(v,bool) or v<0: raise ValueError(f"{name} must be non-negative integer")
        if self.exchange_timestamp_ns is not None and (not isinstance(self.exchange_timestamp_ns,int) or isinstance(self.exchange_timestamp_ns,bool) or self.exchange_timestamp_ns<0): raise ValueError("exchange timestamp invalid")
        for x in (self.cvd,self.volume_profile,self.vpin,self.absorption,self.gex):
            if not isinstance(x,ChannelEvidence): raise TypeError("five typed channel evidence records required")

def normalize_analytics_state(state, *, source_event_ids, reference_price, history_event_count, classified_trade_count):
    """Normalize one AnalyticsState snapshot; provenance is caller/journal supplied."""
    venue, instrument = state.venue, state.instrument_id
    cvd = state.cvd
    if cvd is None or cvd.classified_trade_count < 1:
        cvd_ev = ChannelEvidence("cvd", False, None, "no classified CVD history")
    else:
        raw = cvd.classified_delta_ratio
        cvd_ev = ChannelEvidence("cvd", raw is not None, None if raw is None else max(-1.0,min(1.0,float(raw))), None if raw is not None else "classified delta ratio unavailable")
    profile = state.volume_profile
    if reference_price is None or profile.poc is None:
        profile_ev = ChannelEvidence("volume_profile", False, None, "price or profile levels unavailable")
    elif profile.vah is not None and reference_price > profile.vah:
        profile_ev = ChannelEvidence("volume_profile", True, 1.0, context=(("location","ABOVE_VAH"),))
    elif profile.val is not None and reference_price < profile.val:
        profile_ev = ChannelEvidence("volume_profile", True, -1.0, context=(("location","BELOW_VAL"),))
    else:
        profile_ev = ChannelEvidence("volume_profile", True, 0.0, context=(("location","INSIDE_VALUE"),))
    vpin = state.vpin
    if vpin is None or vpin.current_vpin is None:
        vpin_ev = ChannelEvidence("vpin", False, None, "VPIN bucket history unavailable")
    else:
        rank = vpin.rolling_cdf_toxicity_rank
        penalty = 0.0 if rank is None else max(0.0,min(1.0,(float(rank)-0.5)*2.0))
        vpin_ev = ChannelEvidence("vpin", True, None, "toxicity rank unavailable" if rank is None else None, risk_modifier=penalty,
                                  context=(("current_vpin",str(vpin.current_vpin)),))
    absorption = state.absorption
    if absorption is None or absorption.signal not in ("BUY_ABSORPTION_CANDIDATE","SELL_ABSORPTION_CANDIDATE"):
        absorption_ev = ChannelEvidence("absorption", False, None, "no qualified absorption candidate")
    else:
        val = 1.0 if absorption.signal == "BUY_ABSORPTION_CANDIDATE" else -1.0
        absorption_ev = ChannelEvidence("absorption", True, val, context=(("candidate",absorption.signal),))
    gex = state.gex
    if gex is None or state.gex_as_of_timestamp_ns is None:
        gex_ev = ChannelEvidence("gex", False, None, "GEX not refreshed")
    else:
        gex_ev = ChannelEvidence("gex", True, None, context=(("net_gex",str(gex.net_gex)),("call_wall",str(gex.call_wall)),("put_wall",str(gex.put_wall)),("zero_flip",str(gex.zero_flip)),("dealer_assumption",str(state.dealer_positioning_assumption))))
    return EvidenceBundle(venue,instrument,state.session_id,state.audit_sequence,state.exchange_timestamp_ns,
        state.receive_timestamp_ns,tuple(source_event_ids),cvd_ev,profile_ev,vpin_ev,absorption_ev,gex_ev,
        history_event_count,classified_trade_count)
