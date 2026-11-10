from core.signals.contracts import ExecutionMode,RejectionReason,SignalDirection
from core.signals.evidence import ChannelEvidence,EvidenceBundle
from core.signals.fusion import FusionContext,MicrostructureFusionEngine
from core.signals.fusion_config import DEFAULT_POLICIES
N="NSE:NSE_SPOT:NIFTY:INR"
def bundle(c=0.9,p=0.6,a=0.4,risk=0.0,gex=True,inst=N,exchange=1000000,ids=("j1","j2")):
 return EvidenceBundle("NSE",inst,"s1",55,exchange,1100000,ids,ChannelEvidence("cvd",True,c),ChannelEvidence("volume_profile",True,p),ChannelEvidence("vpin",True,None,risk_modifier=risk),ChannelEvidence("absorption",True,a),ChannelEvidence("gex",gex,None),200,50)
def ctx(**kw):
 d=dict(feed_status="HEALTHY",feed_age_ns=10,book_synchronized=True,gex_as_of_timestamp_ns=900000);d.update(kw);return FusionContext(**d)
def test_bullish_signal_is_auditable_and_disabled():
 r=MicrostructureFusionEngine(DEFAULT_POLICIES[N]).evaluate(bundle(),ctx())
 assert r.accepted and r.direction is SignalDirection.LONG and r.signal.execution_mode is ExecutionMode.DISABLED
 assert r.signal.provenance.source_event_ids==("j1","j2") and r.signal.confidence.calibration_label=="UNCALIBRATED"
def test_bearish_signal():
 assert MicrostructureFusionEngine(DEFAULT_POLICIES[N]).evaluate(bundle(-.9,-.7,-.5),ctx()).direction is SignalDirection.SHORT
def test_conflicting_strong_votes_force_neutral():
 r=MicrostructureFusionEngine(DEFAULT_POLICIES[N]).evaluate(bundle(.9,-.8,0),ctx())
 assert r.direction is SignalDirection.NEUTRAL and "DIRECTIONAL_CONFLICT_NEUTRALIZED" in r.tags
def test_feed_and_book_quality_rejections():
 e=MicrostructureFusionEngine(DEFAULT_POLICIES[N])
 assert RejectionReason.FEED_DESYNCHRONIZED in e.evaluate(bundle(),ctx(feed_status="DESYNCHRONIZED")).rejection_reasons
 assert RejectionReason.BOOK_NOT_SYNCHRONIZED in e.evaluate(bundle(),ctx(book_synchronized=False)).rejection_reasons
def test_wrong_instrument_rejected():
 assert RejectionReason.INSTRUMENT_NOT_ALLOWED in MicrostructureFusionEngine(DEFAULT_POLICIES[N]).evaluate(bundle(inst="NSE:NSE_SPOT:BANKNIFTY:INR"),ctx()).rejection_reasons
def test_vpin_and_gex_do_not_vote_directionally():
 r=MicrostructureFusionEngine(DEFAULT_POLICIES[N]).evaluate(bundle(0,0,0,risk=.8),ctx())
 assert r.direction is SignalDirection.NEUTRAL
def test_missing_gex_is_unavailable_not_zero():
 r=MicrostructureFusionEngine(DEFAULT_POLICIES[N]).evaluate(bundle(gex=False),ctx(gex_as_of_timestamp_ns=None))
 assert r.accepted and "GEX_CONTEXT_PRESENT" not in r.tags
def test_missing_exchange_clock_not_substituted():
 assert RejectionReason.MISSING_EXCHANGE_TIMESTAMP in MicrostructureFusionEngine(DEFAULT_POLICIES[N]).evaluate(bundle(exchange=None),ctx()).rejection_reasons
def test_future_gex_asof_rejected():
 assert RejectionReason.INVALID_CLOCK_ORDER in MicrostructureFusionEngine(DEFAULT_POLICIES[N]).evaluate(bundle(),ctx(gex_as_of_timestamp_ns=2000000)).rejection_reasons
def test_provenance_required():
 try: bundle(ids=())
 except ValueError: pass
 else: raise AssertionError("empty provenance must fail closed")
