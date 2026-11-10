from adapters.base.events import DepthDeltaEvent, DepthSnapshotEvent, DepthSnapshotLevel, EventEnvelope, EventType
from adapters.base.instrument import InstrumentId, InstrumentKind
from pipeline.event_journal import EventJournal
from pipeline.replay_verifier import verify_journals

INST = InstrumentId("NSE", InstrumentKind.NSE_SPOT, "NIFTY")
DOMAIN = "SMARTAPI:DEPTH:1:26000"

def event(event_id, typ, payload, seq, ts):
    return EventEnvelope("NSE", INST, typ, ts, ts + 100, seq, DOMAIN, payload, event_id)

def snapshot(seq, bid=99.0, ask=101.0):
    return DepthSnapshotEvent((DepthSnapshotLevel(bid, 10),),
                              (DepthSnapshotLevel(ask, 12),), seq)

def delta(seq, price=99.0, qty=11.0):
    return event(f"d{seq}", EventType.DEPTH_DELTA,
                 DepthDeltaEvent("BID", price, qty), seq, seq * 1000)

def test_recorded_replay_gap_recovery_is_deterministic(tmp_path):
    path = tmp_path / "nse_fixture.jsonl"
    journal = EventJournal(path)
    journal.append(event("s100", EventType.DEPTH_SNAPSHOT, snapshot(100), 100, 100000))
    journal.append(delta(101))
    journal.append(delta(104))
    journal.append(delta(105))
    journal.append(event("s200", EventType.DEPTH_SNAPSHOT, snapshot(200, 98, 102), 200, 200000))
    journal.append(delta(201, 98, 15))
    first = verify_journals([path])
    second = verify_journals([path])
    assert first == second
    assert first.event_count == 6
    assert first.depth_snapshot_count == 2
    assert first.depth_delta_applied == 2
    assert first.depth_quarantined == 2
    assert first.final_books[0][1]["last_sequence"] == 201
    assert first.final_books[0][1]["bids"] == ((98.0, 15),)

def test_out_of_order_late_packet_stays_quarantined_until_snapshot(tmp_path):
    path = tmp_path / "out_of_order.jsonl"
    journal = EventJournal(path)
    journal.append(event("s10", EventType.DEPTH_SNAPSHOT, snapshot(10), 10, 10000))
    journal.append(delta(11))
    journal.append(delta(13))
    journal.append(delta(12))
    journal.append(event("s20", EventType.DEPTH_SNAPSHOT, snapshot(20), 20, 20000))
    journal.append(delta(21))
    result = verify_journals([path])
    assert result.depth_quarantined == 2
    assert result.depth_delta_applied == 2
    assert result.final_books[0][1]["last_sequence"] == 21
