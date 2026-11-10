from core.signals.evidence import ChannelEvidence,EvidenceBundle
from core.signals.fusion import FusionContext,MicrostructureFusionEngine
from core.signals.fusion_config import DEFAULT_POLICIES
def test_replay_is_deterministic_for_recorded_evidence():
 i="BINANCE:CRYPTO_PERPETUAL:BTCUSDT:USDT";e=MicrostructureFusionEngine(DEFAULT_POLICIES[i])
 b=EvidenceBundle("BINANCE",i,"replay",101,10000,10100,("btc-trade-1","btc-depth-9"),ChannelEvidence("cvd",True,.75),ChannelEvidence("volume_profile",True,.35),ChannelEvidence("vpin",True,None,risk_modifier=.1),ChannelEvidence("absorption",True,.4),ChannelEvidence("gex",False,None),300,100)
 c=FusionContext("HEALTHY",100,True);first=e.evaluate(b,c)
 assert all(e.evaluate(b,c)==first for _ in range(50))
 assert first.signal.provenance.source_event_ids==("btc-trade-1","btc-depth-9")
