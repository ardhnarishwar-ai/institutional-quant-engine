from core.signals.contracts import ExecutionMode,SignalDirection
from core.signals.evidence import ChannelEvidence,EvidenceBundle
from core.signals.fusion import FusionContext,MicrostructureFusionEngine
from core.signals.fusion_config import DEFAULT_POLICIES
N="NSE:NSE_SPOT:NIFTY:INR"
def make(a,b,c):
 return EvidenceBundle("NSE",N,"s",7,10,20,("e1",),ChannelEvidence("cvd",True,a),ChannelEvidence("volume_profile",True,b),ChannelEvidence("vpin",True,None),ChannelEvidence("absorption",True,c),ChannelEvidence("gex",False,None),200,50)
def test_score_coverage_and_execution_invariants():
 e=MicrostructureFusionEngine(DEFAULT_POLICIES[N]);ctx=FusionContext("HEALTHY",1,True)
 for a in (-1,-.5,0,.5,1):
  for b in (-1,-.5,0,.5,1):
   for c in (-1,-.5,0,.5,1):
    r=e.evaluate(make(a,b,c),ctx)
    assert -1<=r.score<=1 and 0<=r.evidence_coverage<=1
    if r.signal: assert r.signal.execution_mode is ExecutionMode.DISABLED
def test_repeat_evaluation_identical():
 e=MicrostructureFusionEngine(DEFAULT_POLICIES[N]);b=make(.7,.5,.3);ctx=FusionContext("HEALTHY",1,True);first=e.evaluate(b,ctx)
 assert all(e.evaluate(b,ctx)==first for _ in range(25))
def test_conflicting_material_votes_never_directional():
 e=MicrostructureFusionEngine(DEFAULT_POLICIES[N]);ctx=FusionContext("HEALTHY",1,True)
 for p in (.3,.5,.8,1):
  for n in (-.3,-.5,-.8,-1):
   assert e.evaluate(make(p,n,0),ctx).direction is SignalDirection.NEUTRAL
