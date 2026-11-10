import pytest
from adapters.base.events import DepthDeltaEvent, DepthSnapshotEvent, DepthSnapshotLevel
from pipeline.depth_recovery import DepthHealth, DepthRecoveryManager

KEY = ("NSE", "NSE:NSE_SPOT:NIFTY:INR", "SMARTAPI:DEPTH:1:26000")

def snapshot(seq=100, bid=99.0, ask=101.0):
    return DepthSnapshotEvent((DepthSnapshotLevel(bid,10),),
                              (DepthSnapshotLevel(ask,12),), seq)

def delta(side="BID", price=99.0, quantity=11.0):
    return DepthDeltaEvent(side, price, quantity)

def test_delta_before_initial_snapshot_is_quarantined():
    m=DepthRecoveryManager()
    r=m.apply_delta(*KEY,delta(),sequence_number=101)
    assert not r.accepted and r.health == DepthHealth.AWAITING_SNAPSHOT
    assert r.reason == "awaiting_initial_snapshot" and m.book(*KEY) is None

def test_snapshot_establishes_boundary_and_delta_applies():
    m=DepthRecoveryManager()
    assert m.apply_snapshot(*KEY,snapshot(100)).accepted
    r=m.apply_delta(*KEY,delta(),sequence_number=101)
    assert r.accepted and m.book(*KEY)["last_sequence"] == 101
    assert m.book(*KEY)["bids"][0] == (99.0,11.0)

def test_gap_quarantines_gap_delta_and_followups():
    m=DepthRecoveryManager()
    m.apply_snapshot(*KEY,snapshot(100))
    r=m.apply_delta(*KEY,delta(),sequence_number=103)
    assert not r.accepted and r.sequence_gap == (101,102)
    assert r.health == DepthHealth.DESYNCHRONIZED
    later=m.apply_delta(*KEY,delta(),sequence_number=104)
    assert not later.accepted and later.reason == "awaiting_recovery_snapshot"
    assert m.book(*KEY) is None

def test_fresh_snapshot_recovers_and_replaces_old_book():
    m=DepthRecoveryManager()
    m.apply_snapshot(*KEY,snapshot(100))
    m.apply_delta(*KEY,delta(),sequence_number=103)
    r=m.apply_snapshot(*KEY,snapshot(200,98.0,102.0))
    assert r.accepted and r.recovered
    b=m.book(*KEY)
    assert b["last_sequence"] == 200
    assert b["bids"] == ((98.0,10),) and b["asks"] == ((102.0,12),)

def test_unsequenced_snapshot_does_not_invent_sequence_baseline():
    m=DepthRecoveryManager()
    snap=DepthSnapshotEvent((DepthSnapshotLevel(99.0,10),),
                            (DepthSnapshotLevel(101.0,12),),None)
    m.apply_snapshot(*KEY,snap)
    assert m.book(*KEY)["last_sequence"] is None
    assert m.apply_delta(*KEY,delta(),sequence_number=900).accepted
    r=m.apply_delta(*KEY,delta(),sequence_number=902)
    assert not r.accepted and r.sequence_gap == (901,901)

def test_duplicate_sequence_is_not_applied():
    m=DepthRecoveryManager()
    m.apply_snapshot(*KEY,snapshot(10))
    m.apply_delta(*KEY,delta(),sequence_number=11)
    r=m.apply_delta(*KEY,delta(),sequence_number=11)
    assert not r.accepted and r.reason == "duplicate_or_regressed_sequence"

def test_crossed_snapshot_rejected_without_mutating_existing_book():
    m=DepthRecoveryManager()
    m.apply_snapshot(*KEY,snapshot(10))
    with pytest.raises(ValueError,match="crossed"):
        m.apply_snapshot(*KEY,snapshot(11,102.0,101.0))
    assert m.book(*KEY)["last_sequence"] == 10

def test_sequence_domains_are_isolated():
    m=DepthRecoveryManager()
    m.apply_snapshot(*KEY,snapshot(10))
    assert m.health(KEY[0],KEY[1],"OTHER") == DepthHealth.AWAITING_SNAPSHOT

def test_journal_metadata_records_gap_and_recovery(tmp_path):
    from pipeline.event_journal import EventJournal
    j=EventJournal(tmp_path/"depth.jsonl")
    m=DepthRecoveryManager(journal=j)
    m.apply_snapshot(*KEY,snapshot(10))
    m.apply_delta(*KEY,delta(),sequence_number=12)
    m.apply_snapshot(*KEY,snapshot(20))
    lines=(tmp_path/"depth.jsonl").read_text().splitlines()
    assert any("DEPTH_SEQUENCE_GAP" in x for x in lines)
    assert any("DEPTH_SNAPSHOT_RECOVERY" in x for x in lines)
