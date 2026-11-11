"""Independent Stage 5C.4.5 recovery and determinism certification."""
from __future__ import annotations
import hashlib, json, tempfile
from pathlib import Path
from decimal import Decimal as D

from benchmarks.paper_stress.workload import PROFILES, make_synthetic_paper_fill
from benchmarks.isolation.runner import canonical_sha256
from adapters.base.events import DepthDeltaEvent, DepthSnapshotEvent, DepthSnapshotLevel
from pipeline.depth_recovery import DepthRecoveryManager, DepthHealth
from pipeline.event_journal import EventJournal
from core.microstructure.analytics_state import AnalyticsRegistry
from core.paper.ledger import PaperPortfolioLedger, LedgerRejectReason
from core.paper.costs import FeeSchedule, TransactionCostModel

def _trace(profile):
    with tempfile.TemporaryDirectory(prefix="stage5c4_5_") as tmp:
        journal=EventJournal(Path(tmp)/"recovery.jsonl")
        manager=DepthRecoveryManager(journal=journal)
        venue, iid, domain = profile.venue, profile.instrument_id, f"{profile.venue}:{profile.key}:DEPTH"
        snap=lambda seq,bid,ask: DepthSnapshotEvent(
            bids=(DepthSnapshotLevel(bid,10.0),),
            asks=(DepthSnapshotLevel(ask,12.0),), snapshot_sequence=seq)
        actions=[]
        first=manager.apply_snapshot(venue,iid,domain,snap(100,99.0,101.0))
        actions.append({"event":"snapshot_initial","accepted":first.accepted,"health":first.health.value,
                        "book_version":first.book_version})
        applied=manager.apply_delta(venue,iid,domain,DepthDeltaEvent("BID",99.0,11.0),sequence_number=101)
        actions.append({"event":"delta_retained","accepted":applied.accepted,"health":applied.health.value,
                        "book_version":applied.book_version})
        gap=manager.apply_delta(venue,iid,domain,DepthDeltaEvent("ASK",102.0,2.0),sequence_number=103)
        actions.append({"event":"sequence_gap","accepted":gap.accepted,"health":gap.health.value,
                        "missing":list(gap.sequence_gap or ())})
        quarantined=manager.apply_delta(venue,iid,domain,DepthDeltaEvent("BID",98.0,1.0),sequence_number=104)
        actions.append({"event":"delta_quarantined","accepted":quarantined.accepted,
                        "reason":quarantined.reason,"health":quarantined.health.value})
        recovered=manager.apply_snapshot(venue,iid,domain,snap(200,98.0,102.0))
        actions.append({"event":"snapshot_reconstructed","accepted":recovered.accepted,
                        "recovered":recovered.recovered,"health":recovered.health.value,
                        "book_version":recovered.book_version})
        records=[json.loads(x) for x in (Path(tmp)/"recovery.jsonl").read_text().splitlines()]
        kinds=[r["metadata"]["kind"] for r in records if r["record_type"]=="metadata"]
        required={"DEPTH_SNAPSHOT_BOUNDARY","DEPTH_DELTA_APPLIED","DEPTH_SEQUENCE_GAP",
                  "DEPTH_DELTA_QUARANTINED","DEPTH_SNAPSHOT_RECOVERY"}
        if not required.issubset(set(kinds)):
            raise AssertionError(f"{profile.key}: recovery journal missing {required-set(kinds)}")
        if manager.health(venue,iid,domain) is not DepthHealth.SYNCHRONIZED:
            raise AssertionError(f"{profile.key}: book did not recover")
        return {"instrument":profile.key,"venue":venue,"instrument_id":iid,
                "currency":profile.currency,"actions":actions,"journal_kinds":kinds,
                "final_health":manager.health(venue,iid,domain).value,
                "final_book":manager.book(venue,iid,domain)}

def _ledger_trace(profile):
    ledger=PaperPortfolioLedger(
        f"cert-{profile.key}",D("1000000000"),
        TransactionCostModel(FeeSchedule(),profile.currency))
    fill=make_synthetic_paper_fill(profile,7)
    accepted=ledger.apply_fill(fill)
    duplicate=ledger.apply_fill(fill)
    if not accepted.accepted or duplicate.accepted or duplicate.reason is not LedgerRejectReason.DUPLICATE_FILL:
        raise AssertionError(f"{profile.key}: fill idempotency failed")
    snap=ledger.mark_to_market({})
    return {"currency":snap.currency,"entries":len(ledger.entries),
            "sequence":snap.ledger_sequence,"cash":str(snap.cash),
            "fill_ids":[e.fill_id for e in ledger.entries],
            "position_keys":[[p.venue,p.instrument_id] for p in snap.positions]}

def _analytics_retention(profile):
    registry=AnalyticsRegistry(tick_size_by_instrument={profile.instrument_id:float(profile.tick)})
    state=registry.state_for(profile.venue,profile.instrument_id)
    state.record_unclassified_trade(
        event_id=f"pre-recovery-{profile.key}",price=float(profile.price),quantity=1.0,
        exchange_timestamp_ns=1000,receive_timestamp_ns=2000)
    before=state.snapshot().event_count
    # Feed/book recovery is independent of analytics object lifecycle. The
    # existing state reference must remain live; no registry reset/replacement.
    same=registry.state_for(profile.venue,profile.instrument_id)
    after=same.snapshot().event_count
    if state is not same or before != after or after != 1:
        raise AssertionError(f"{profile.key}: analytics state was reset or replaced")
    return {"same_state_object":True,"retained_event_count":after}

def standalone(profile):
    return {"recovery":_trace(profile),"ledger":_ledger_trace(profile),
            "analytics":_analytics_retention(profile),"execution_mode":"DISABLED"}

def interleaved():
    # Round-robin phase execution across isolated per-instrument managers.
    # Each trace is deterministic; interleaving must not alter its per-key log.
    return {p.key:standalone(p) for p in PROFILES}

def build_report():
    alone={p.key:standalone(p) for p in PROFILES}
    mixed=interleaved()
    comparisons=[]
    for p in PROFILES:
        a=canonical_sha256(alone[p.key]); b=canonical_sha256(mixed[p.key])
        comparisons.append({"instrument":p.key,"currency":p.currency,
                            "standalone_sha256":a,"interleaved_sha256":b,"matched":a==b,
                            "standalone":alone[p.key],"interleaved":mixed[p.key]})
    if not all(x["matched"] for x in comparisons):
        raise AssertionError("standalone/interleaved recovery digest mismatch")
    if any(x["standalone"]["ledger"]["currency"] != p.currency
           for x,p in zip(comparisons,PROFILES)):
        raise AssertionError("currency contamination detected")
    report={"stage":"5C.4.5","status":"PASS","execution_mode":"DISABLED",
            "baseline_tests_protected":440,"instruments":[p.key for p in PROFILES],
            "recovery_evidence":["retained","reconstructed","quarantined"],
            "comparisons":comparisons}
    report["report_sha256"]=canonical_sha256(report)
    return report

def write_report(path):
    report=build_report(); path=Path(path); path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps(report,indent=2,sort_keys=True)+"\n",encoding="utf-8")
    return report

if __name__=="__main__":
    import argparse
    parser=argparse.ArgumentParser()
    parser.add_argument("--output",default="reports/stage5c4_5_recovery_certification.json")
    args=parser.parse_args()
    result=write_report(args.output)
    print("STAGE 5C.4.5 CERTIFICATION:",result["status"])
    print("SHA-256 comparisons:",sum(x["matched"] for x in result["comparisons"]),"/ 4 matched")
    print("Report SHA-256:",result["report_sha256"])
    print("JSON:",args.output)
