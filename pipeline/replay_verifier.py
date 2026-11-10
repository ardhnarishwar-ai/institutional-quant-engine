"""Replay canonical journal events through validation and depth recovery."""
from dataclasses import dataclass
from adapters.base.events import EventType
from adapters.replay.reader import ReplayReader
from pipeline.depth_recovery import DepthRecoveryManager
from pipeline.event_validator import EventValidator

@dataclass(frozen=True, slots=True)
class ReplayVerification:
    event_count: int
    accepted_count: int
    rejected_count: int
    depth_snapshot_count: int
    depth_delta_applied: int
    depth_quarantined: int
    final_books: tuple
    event_ids: tuple

def verify_journals(paths, metadata_journal=None):
    reader = ReplayReader(paths)
    validator = EventValidator(journal=metadata_journal)
    depth = DepthRecoveryManager(journal=metadata_journal)
    accepted = rejected = snapshots = applied = quarantined = 0
    ids = []
    book_keys = set()
    for event in reader.events():
        ids.append(event.event_id)
        result = validator.validate(event)
        if not result.accepted:
            rejected += 1
            quarantined += int(result.quarantined)
            continue
        if event.event_type == EventType.DEPTH_SNAPSHOT:
            snapshots += 1
            key = (event.venue, event.instrument.canonical_id, event.sequence_domain or "SMARTAPI:DEPTH:UNSEQUENCED")
            depth.apply_snapshot(*key, event.payload)
            book_keys.add(key)
            accepted += 1
        elif event.event_type == EventType.DEPTH_DELTA:
            key = (event.venue, event.instrument.canonical_id, event.sequence_domain)
            if event.sequence_number is None:
                rejected += 1
                quarantined += 1
                continue
            result = depth.apply_delta(*key, event.payload, sequence_number=event.sequence_number)
            if result.accepted:
                applied += 1
                accepted += 1
            else:
                rejected += 1
                quarantined += 1
            book_keys.add(key)
        else:
            accepted += 1
    final = []
    for key in sorted(book_keys):
        state = depth.book(*key)
        if state is not None:
            final.append(("|".join(key), state))
    return ReplayVerification(len(ids), accepted, rejected, snapshots, applied,
                               quarantined, tuple(final), tuple(ids))
