from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum
from adapters.base.events import DepthSnapshotEvent, DepthDeltaEvent
from adapters.base.depth_batch import DepthDeltaBatchEvent

class DepthHealth(str, Enum):
    AWAITING_SNAPSHOT = "AWAITING_SNAPSHOT"
    SYNCHRONIZED = "SYNCHRONIZED"
    DESYNCHRONIZED = "DESYNCHRONIZED"

class DepthRecoveryError(ValueError):
    pass

@dataclass(frozen=True, slots=True)
class DepthResult:
    accepted: bool
    health: DepthHealth
    reason: str
    sequence_gap: tuple[int, int] | None = None
    recovered: bool = False
    book_version: int = 0
    depth_bridged: bool = False

@dataclass
class _Book:
    health: DepthHealth = DepthHealth.AWAITING_SNAPSHOT
    bids: dict[float, float] = field(default_factory=dict)
    asks: dict[float, float] = field(default_factory=dict)
    last_sequence: int | None = None
    book_version: int = 0
    depth_bridged: bool = False

class DepthRecoveryManager:
    # One isolated book per venue, canonical instrument and sequence domain.
    def __init__(self, journal=None):
        self._books = {}
        self._journal = journal

    @staticmethod
    def _key(venue, instrument_id, sequence_domain):
        if not all(isinstance(x, str) and x.strip() for x in (venue, instrument_id, sequence_domain)):
            raise ValueError("venue, instrument_id and sequence_domain are required")
        return venue.upper(), instrument_id, sequence_domain

    def _record(self, key, kind, **fields):
        item = {"kind": kind, "venue": key[0], "instrument_id": key[1],
                "sequence_domain": key[2], **fields}
        if self._journal is not None:
            self._journal.append_metadata(item)
        return item

    def health(self, venue, instrument_id, sequence_domain):
        key = self._key(venue, instrument_id, sequence_domain)
        state = self._books.get(key)
        return state.health if state else DepthHealth.AWAITING_SNAPSHOT

    def apply_snapshot(self, venue, instrument_id, sequence_domain, snapshot):
        if not isinstance(snapshot, DepthSnapshotEvent):
            raise TypeError("snapshot must be DepthSnapshotEvent")
        key = self._key(venue, instrument_id, sequence_domain)
        old = self._books.get(key)
        recovered = old is not None and old.health == DepthHealth.DESYNCHRONIZED
        bids = {x.price: x.quantity for x in snapshot.bids}
        asks = {x.price: x.quantity for x in snapshot.asks}
        if not bids or not asks:
            raise DepthRecoveryError("snapshot must contain at least one bid and one ask")
        if max(bids) >= min(asks):
            raise DepthRecoveryError("snapshot is crossed")
        # Atomic replacement; stale levels are never merged into a fresh snapshot.
        book = _Book(
            health=DepthHealth.SYNCHRONIZED,
            bids=bids,
            asks=asks,
            last_sequence=snapshot.snapshot_sequence,
            book_version=(old.book_version + 1 if old else 1),
            depth_bridged=False,
        )
        self._books[key] = book
        kind = "DEPTH_SNAPSHOT_RECOVERY" if recovered else "DEPTH_SNAPSHOT_BOUNDARY"
        self._record(key, kind, snapshot_sequence=snapshot.snapshot_sequence,
                     sequence_tracking=snapshot.snapshot_sequence is not None,
                     book_version=book.book_version)
        return DepthResult(True, book.health, kind, recovered=recovered,
                           book_version=book.book_version)

    def apply_delta(self, venue, instrument_id, sequence_domain, delta, *, sequence_number):
        if not isinstance(delta, DepthDeltaEvent):
            raise TypeError("delta must be DepthDeltaEvent")
        if not isinstance(sequence_number, int) or isinstance(sequence_number, bool) or sequence_number < 0:
            raise ValueError("sequence_number must be a non-negative integer")
        key = self._key(venue, instrument_id, sequence_domain)
        book = self._books.get(key)
        if book is None or book.health == DepthHealth.AWAITING_SNAPSHOT:
            self._record(key, "DEPTH_DELTA_QUARANTINED", sequence_number=sequence_number,
                         reason="awaiting_initial_snapshot")
            return DepthResult(False, DepthHealth.AWAITING_SNAPSHOT, "awaiting_initial_snapshot")
        if book.health == DepthHealth.DESYNCHRONIZED:
            self._record(key, "DEPTH_DELTA_QUARANTINED", sequence_number=sequence_number,
                         reason="awaiting_recovery_snapshot")
            return DepthResult(False, book.health, "awaiting_recovery_snapshot",
                               book_version=book.book_version)
        # With a sequence-less snapshot, first observed delta establishes only
        # a prospective baseline; a gap before that first delta is unknowable.
        if book.last_sequence is None:
            book.last_sequence = sequence_number
            self._record(key, "DEPTH_SEQUENCE_BASELINE", sequence_number=sequence_number,
                         reason="first_sequence_after_unsequenced_snapshot")
        else:
            expected = book.last_sequence + 1
            if sequence_number > expected:
                gap = (expected, sequence_number - 1)
                book.health = DepthHealth.DESYNCHRONIZED
                self._record(key, "DEPTH_SEQUENCE_GAP", missing_from=gap[0],
                             missing_to=gap[1], observed_sequence=sequence_number,
                             action="DESYNCHRONIZED")
                self._record(key, "DEPTH_DELTA_QUARANTINED",
                             sequence_number=sequence_number, reason="sequence_gap")
                return DepthResult(False, book.health, "sequence_gap",
                                   sequence_gap=gap, book_version=book.book_version)
            if sequence_number <= book.last_sequence:
                self._record(key, "DEPTH_DELTA_QUARANTINED", sequence_number=sequence_number,
                             reason="duplicate_or_regressed_sequence")
                return DepthResult(False, book.health, "duplicate_or_regressed_sequence",
                                   book_version=book.book_version)
        side = book.bids if delta.side == "BID" else book.asks
        if delta.action == "DELETE":
            side.pop(delta.price, None)
        else:
            side[delta.price] = delta.quantity
        if book.bids and book.asks and max(book.bids) >= min(book.asks):
            book.health = DepthHealth.DESYNCHRONIZED
            self._record(key, "DEPTH_BOOK_INVALID", sequence_number=sequence_number,
                         reason="crossed_book", action="DESYNCHRONIZED")
            return DepthResult(False, book.health, "crossed_book",
                               book_version=book.book_version)
        book.last_sequence = sequence_number
        book.book_version += 1
        self._record(key, "DEPTH_DELTA_APPLIED", sequence_number=sequence_number,
                     book_version=book.book_version)
        return DepthResult(True, book.health, "applied", book_version=book.book_version)

    def apply_batch(self, venue, instrument_id, sequence_domain, batch):
        if not isinstance(batch, DepthDeltaBatchEvent):
            raise TypeError("batch must be DepthDeltaBatchEvent")
        key = self._key(venue, instrument_id, sequence_domain)
        book = self._books.get(key)
        seq = batch.final_update_id
        if book is None or book.health == DepthHealth.AWAITING_SNAPSHOT:
            self._record(key, "DEPTH_DELTA_QUARANTINED", sequence_number=seq, reason="awaiting_initial_snapshot")
            return DepthResult(False, DepthHealth.AWAITING_SNAPSHOT, "awaiting_initial_snapshot")
        if book.health == DepthHealth.DESYNCHRONIZED:
            self._record(key, "DEPTH_DELTA_QUARANTINED", sequence_number=seq, reason="awaiting_recovery_snapshot")
            return DepthResult(False, book.health, "awaiting_recovery_snapshot", book_version=book.book_version)
        if book.last_sequence is not None:
            if not book.depth_bridged:
                valid = batch.first_update_id <= book.last_sequence + 1 <= batch.final_update_id
            else:
                valid = batch.previous_final_update_id == book.last_sequence
            if not valid:
                gap = (book.last_sequence + 1, max(book.last_sequence + 1, batch.first_update_id - 1))
                book.health = DepthHealth.DESYNCHRONIZED
                self._record(key, "DEPTH_SEQUENCE_GAP", missing_from=gap[0], missing_to=gap[1], observed_sequence=seq, action="DESYNCHRONIZED")
                self._record(key, "DEPTH_DELTA_QUARANTINED", sequence_number=seq, reason="sequence_gap")
                return DepthResult(False, book.health, "sequence_gap", sequence_gap=gap, book_version=book.book_version)
        bids, asks = dict(book.bids), dict(book.asks)
        for change in batch.changes:
            side = bids if change.side == "BID" else asks
            if change.action == "DELETE": side.pop(change.price, None)
            else: side[change.price] = change.quantity
        if bids and asks and max(bids) >= min(asks):
            book.health = DepthHealth.DESYNCHRONIZED
            self._record(key, "DEPTH_BOOK_INVALID", sequence_number=seq, reason="crossed_book", action="DESYNCHRONIZED")
            return DepthResult(False, book.health, "crossed_book", book_version=book.book_version)
        book.bids, book.asks = bids, asks
        book.last_sequence = seq
        book.depth_bridged = True
        book.book_version += 1
        self._record(key, "DEPTH_DELTA_BATCH_APPLIED", sequence_number=seq, change_count=len(batch.changes), book_version=book.book_version)
        return DepthResult(True, book.health, "applied", book_version=book.book_version)

    def book(self, venue, instrument_id, sequence_domain):
        key = self._key(venue, instrument_id, sequence_domain)
        state = self._books.get(key)
        if state is None or state.health != DepthHealth.SYNCHRONIZED:
            return None
        return {"bids": tuple(sorted(state.bids.items(), reverse=True)),
                "asks": tuple(sorted(state.asks.items())),
                "last_sequence": state.last_sequence,
                "book_version": state.book_version}
