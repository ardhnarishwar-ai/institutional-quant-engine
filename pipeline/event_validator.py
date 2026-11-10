"""Sequence-aware event validation with depth-delta quarantine and recovery."""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

from adapters.base.events import EventEnvelope, EventType


class EventValidationError(ValueError):
    pass


class StreamHealth(str, Enum):
    SYNCHRONIZED = "SYNCHRONIZED"
    DESYNCHRONIZED = "DESYNCHRONIZED"


@dataclass(frozen=True, slots=True)
class ValidationResult:
    accepted: bool
    sequence_gap: Optional[tuple[int, int]] = None
    quarantined: bool = False
    recovered: bool = False
    metadata: dict = field(default_factory=dict)
    causal_parent_pending: bool = False


@dataclass
class _StreamState:
    exchange_timestamp_ns: int
    receive_timestamp_ns: int
    sequence_number: Optional[int]
    health: StreamHealth = StreamHealth.SYNCHRONIZED


class EventValidator:
    """Validate each independent sequence domain; depth gaps require snapshot recovery.

    Stream identity is venue + canonical instrument + sequence domain. A gap
    marks that stream desynchronized. Dependent depth deltas are quarantined
    until a valid DEPTH_SNAPSHOT arrives on that same stream. Snapshot sequence
    establishes a new baseline; unrelated domains remain independent.
    """

    def __init__(self, journal=None):
        self._streams: dict[tuple, _StreamState] = {}
        self._events: dict[str, EventEnvelope] = {}
        self._pending_children: dict[str, list[EventEnvelope]] = {}
        self._journal = journal
        self._history: list[dict] = []

    @staticmethod
    def _stream_key(event):
        return (event.venue, event.instrument.canonical_id, event.sequence_domain)

    def _record(self, metadata):
        item = dict(metadata)
        self._history.append(item)
        if self._journal is not None:
            self._journal.append_metadata(item)

    def stream_health(self, event_or_key):
        key = self._stream_key(event_or_key) if isinstance(event_or_key, EventEnvelope) else event_or_key
        state = self._streams.get(key)
        return state.health if state else StreamHealth.SYNCHRONIZED

    def validate(self, event: EventEnvelope) -> ValidationResult:
        if not isinstance(event, EventEnvelope):
            raise TypeError("event must be EventEnvelope")
        if event.event_id in self._events:
            raise EventValidationError(f"duplicate event_id: {event.event_id}")

        # Validate causal ordering independently of transport sequence domains.
        # A child may arrive before its parent; retain it as pending and check
        # the timestamp relationship when the parent eventually arrives.
        parent_id = event.causal_parent_id
        causal_parent_pending = False
        if parent_id:
            parent = self._events.get(parent_id)
            if parent is not None:
                if (parent.exchange_timestamp_ns is not None and event.exchange_timestamp_ns is not None
                        and parent.exchange_timestamp_ns > event.exchange_timestamp_ns):
                    raise EventValidationError("causal parent exchange timestamp is later than child")
                if parent.receive_timestamp_ns > event.receive_timestamp_ns:
                    raise EventValidationError("causal parent receive timestamp is later than child")
            else:
                causal_parent_pending = True

        pending_children = self._pending_children.get(event.event_id, ())
        for child in pending_children:
            if (event.exchange_timestamp_ns is not None and child.exchange_timestamp_ns is not None
                    and event.exchange_timestamp_ns > child.exchange_timestamp_ns):
                raise EventValidationError("causal parent exchange timestamp is later than pending child")
            if event.receive_timestamp_ns > child.receive_timestamp_ns:
                raise EventValidationError("causal parent receive timestamp is later than pending child")

        key = self._stream_key(event)
        previous = self._streams.get(key)
        is_depth_delta = event.event_type == EventType.DEPTH_DELTA
        is_snapshot = event.event_type == EventType.DEPTH_SNAPSHOT

        if previous and previous.health == StreamHealth.DESYNCHRONIZED:
            if is_depth_delta:
                meta = {
                    "kind": "DEPTH_DELTA_QUARANTINED",
                    "event_id": event.event_id,
                    "venue": event.venue,
                    "instrument_id": event.instrument.canonical_id,
                    "sequence_domain": event.sequence_domain,
                    "sequence_number": event.sequence_number,
                    "reason": "awaiting_fresh_depth_snapshot",
                    "exchange_timestamp_ns": event.exchange_timestamp_ns,
                    "receive_timestamp_ns": event.receive_timestamp_ns,
                }
                self._record(meta)
                return ValidationResult(False, quarantined=True, metadata=meta)
            if not is_snapshot:
                # Non-depth events sharing a sequence domain do not repair or
                # worsen a broken order-book stream; retain audit visibility.
                meta = {"kind": "EVENT_WHILE_DESYNCHRONIZED",
                        "event_id": event.event_id, "sequence_domain": event.sequence_domain}
                self._record(meta)
                return ValidationResult(False, quarantined=True, metadata=meta)

        if previous:
            if (event.exchange_timestamp_ns is not None and previous.exchange_timestamp_ns is not None
                    and event.exchange_timestamp_ns < previous.exchange_timestamp_ns):
                raise EventValidationError("exchange timestamp regressed within stream")
            if event.receive_timestamp_ns < previous.receive_timestamp_ns:
                raise EventValidationError("receive timestamp regressed within stream")

        gap = None
        recovered = False
        incoming_seq = event.sequence_number
        if is_snapshot:
            snapshot_seq = event.payload.snapshot_sequence
            if incoming_seq is not None and incoming_seq != snapshot_seq:
                raise EventValidationError("envelope sequence must equal snapshot_sequence")
            incoming_seq = snapshot_seq
            recovered = previous is not None and previous.health == StreamHealth.DESYNCHRONIZED
        elif previous and incoming_seq is not None and previous.sequence_number is not None:
            expected = previous.sequence_number + 1
            if incoming_seq > expected:
                gap = (expected, incoming_seq - 1)
            elif incoming_seq < expected:
                raise EventValidationError("sequence number regressed or duplicated")

        if gap is not None:
            meta = {
                "kind": "SEQUENCE_GAP",
                "event_id": event.event_id,
                "venue": event.venue,
                "instrument_id": event.instrument.canonical_id,
                "sequence_domain": event.sequence_domain,
                "missing_from": gap[0],
                "missing_to": gap[1],
                "observed_sequence": incoming_seq,
                "exchange_timestamp_ns": event.exchange_timestamp_ns,
                "receive_timestamp_ns": event.receive_timestamp_ns,
                "action": "DESYNCHRONIZED",
            }
            self._record(meta)
            self._streams[key] = _StreamState(
                event.exchange_timestamp_ns, event.receive_timestamp_ns,
                incoming_seq, StreamHealth.DESYNCHRONIZED)
            self._events[event.event_id] = event
            # The gap-causing depth delta is itself quarantined.
            if is_depth_delta:
                qmeta = dict(meta, kind="DEPTH_DELTA_QUARANTINED",
                             reason="sequence_gap_on_this_event")
                self._record(qmeta)
                return ValidationResult(False, gap, True, False, qmeta)
            return ValidationResult(False, gap, False, False, meta)

        if previous and previous.health == StreamHealth.DESYNCHRONIZED and is_snapshot:
            meta = {
                "kind": "SEQUENCE_RECOVERY",
                "event_id": event.event_id,
                "venue": event.venue,
                "instrument_id": event.instrument.canonical_id,
                "sequence_domain": event.sequence_domain,
                "recovered_at_sequence": incoming_seq,
                "exchange_timestamp_ns": event.exchange_timestamp_ns,
                "receive_timestamp_ns": event.receive_timestamp_ns,
                "action": "SYNCHRONIZED",
            }
            self._record(meta)
            self._streams[key] = _StreamState(
                event.exchange_timestamp_ns, event.receive_timestamp_ns,
                incoming_seq, StreamHealth.SYNCHRONIZED)
            self._events[event.event_id] = event
            self._pending_children.pop(event.event_id, None)
            if causal_parent_pending:
                self._pending_children.setdefault(parent_id, []).append(event)
            return ValidationResult(True, recovered=True, metadata=meta,
                                    causal_parent_pending=causal_parent_pending)

        self._streams[key] = _StreamState(
            event.exchange_timestamp_ns, event.receive_timestamp_ns,
            incoming_seq, StreamHealth.SYNCHRONIZED)
        self._events[event.event_id] = event
        self._pending_children.pop(event.event_id, None)
        if causal_parent_pending:
            self._pending_children.setdefault(parent_id, []).append(event)
        return ValidationResult(True, metadata={"kind": "ACCEPTED", "event_id": event.event_id},
                                causal_parent_pending=causal_parent_pending)

    def history(self) -> tuple[dict, ...]:
        return tuple(dict(item) for item in self._history)

    def unresolved_causal_children(self):
        return {parent: tuple(child.event_id for child in children)
                for parent, children in self._pending_children.items()}
