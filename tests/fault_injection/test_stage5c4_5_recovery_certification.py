from decimal import Decimal as D
import pytest
from benchmarks.paper_stress.workload import PROFILES, make_synthetic_paper_fill
from benchmarks.recovery_certification.runner import (
    _trace, _ledger_trace, _analytics_retention, build_report, canonical_sha256,
)
from core.paper.ledger import PaperPortfolioLedger, LedgerRejectReason
from core.paper.costs import FeeSchedule, TransactionCostModel

@pytest.mark.parametrize("profile",PROFILES,ids=lambda p:p.key)
def test_feed_gap_is_quarantined_then_explicitly_reconstructed(profile):
    result=_trace(profile)
    names=[x["event"] for x in result["actions"]]
    assert "delta_quarantined" in names
    assert "snapshot_reconstructed" in names
    assert result["final_health"]=="SYNCHRONIZED"
    assert "DEPTH_SEQUENCE_GAP" in result["journal_kinds"]
    assert "DEPTH_DELTA_QUARANTINED" in result["journal_kinds"]
    assert "DEPTH_SNAPSHOT_RECOVERY" in result["journal_kinds"]

@pytest.mark.parametrize("profile",PROFILES,ids=lambda p:p.key)
def test_analytics_state_is_retained_not_silently_reset(profile):
    result=_analytics_retention(profile)
    assert result=={"same_state_object":True,"retained_event_count":1}

@pytest.mark.parametrize("profile",PROFILES,ids=lambda p:p.key)
def test_ledger_fill_idempotency_and_currency_after_recovery(profile):
    result=_ledger_trace(profile)
    assert result["entries"]==1
    assert result["sequence"]==1
    assert result["currency"]==profile.currency
    assert len(result["fill_ids"])==len(set(result["fill_ids"]))==1

def test_inr_and_usdt_ledgers_remain_separate():
    inr=PaperPortfolioLedger("cert-INR",D("1000000"),TransactionCostModel(FeeSchedule(),"INR"))
    usdt=PaperPortfolioLedger("cert-USDT",D("1000000"),TransactionCostModel(FeeSchedule(),"USDT"))
    for i,p in enumerate(PROFILES):
        ledger=inr if p.currency=="INR" else usdt
        assert ledger.apply_fill(make_synthetic_paper_fill(p,i)).accepted
    assert inr.cost_model.currency=="INR" and usdt.cost_model.currency=="USDT"
    assert all(p.currency=="INR" for p in inr.positions)
    assert all(p.currency=="USDT" for p in usdt.positions)
    assert inr.account_id != usdt.account_id

def test_standalone_and_interleaved_recovery_digests_match_all_four():
    report=build_report()
    assert report["status"]=="PASS"
    assert len(report["comparisons"])==4
    assert all(row["matched"] for row in report["comparisons"])
    unsigned={k:v for k,v in report.items() if k!="report_sha256"}
    assert report["report_sha256"]==canonical_sha256(unsigned)
    assert report["execution_mode"]=="DISABLED"
