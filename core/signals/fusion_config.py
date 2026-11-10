from __future__ import annotations
from dataclasses import dataclass
from typing import Mapping, Tuple

@dataclass(frozen=True, slots=True)
class FusionPolicy:
    instrument_id: str
    weights: Tuple[Tuple[str,float], ...] = (("cvd",0.60),("volume_profile",0.25),("absorption",0.15))
    minimum_coverage: float = 0.60
    minimum_absolute_score: float = 0.20
    minimum_directional_votes: int = 1
    conflict_tolerance: float = 0.20
    max_vpin_risk_penalty: float = 0.20
    max_gex_age_ns: int = 60_000_000_000
    minimum_history_events: int = 100
    minimum_classified_trades: int = 20
    max_feed_age_ns: int = 2_000_000_000
    def __post_init__(self):
        if not self.instrument_id: raise ValueError("instrument_id required")
        if {n for n,_ in self.weights} != {"cvd","volume_profile","absorption"} or len(self.weights)!=3: raise ValueError("define three unique directional weights")
        if any(isinstance(w,bool) or not isinstance(w,(int,float)) or w<=0 for _,w in self.weights): raise ValueError("weights must be positive")
        for n in ("minimum_coverage","minimum_absolute_score","conflict_tolerance","max_vpin_risk_penalty"):
            if not 0<=getattr(self,n)<=1: raise ValueError(f"{n} outside [0,1]")
        for n in ("minimum_directional_votes","max_gex_age_ns","minimum_history_events","minimum_classified_trades","max_feed_age_ns"):
            v=getattr(self,n)
            if not isinstance(v,int) or isinstance(v,bool) or v<0: raise ValueError(f"{n} invalid")
        if min(self.minimum_directional_votes,self.max_gex_age_ns,self.max_feed_age_ns)<1: raise ValueError("vote and age limits must be positive")

DEFAULT_POLICIES: Mapping[str,FusionPolicy] = {
 "NSE:NSE_SPOT:NIFTY:INR": FusionPolicy("NSE:NSE_SPOT:NIFTY:INR"),
 "NSE:NSE_SPOT:BANKNIFTY:INR": FusionPolicy("NSE:NSE_SPOT:BANKNIFTY:INR"),
 "NSE:NSE_SPOT:FINNIFTY:INR": FusionPolicy("NSE:NSE_SPOT:FINNIFTY:INR"),
 "BINANCE:CRYPTO_PERPETUAL:BTCUSDT:USDT": FusionPolicy("BINANCE:CRYPTO_PERPETUAL:BTCUSDT:USDT",weights=(("cvd",0.55),("volume_profile",0.25),("absorption",0.20)),max_gex_age_ns=300_000_000_000),
}
