"""Per-stream monotonicity, sequence-gap and explicit causal-parent validation."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from adapters.base.events import EventEnvelope


class EventValidationError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class ValidationResult:
    accepted: bool
    sequence_gap: Optional[tuple[int, int]] = None
    causal_parent_pending: bool = False


@dataclass
class _StreamState:
    exchange_timestamp_ns: int
    receive_timestamp_ns: int
    sequence_number: Optional[int]


class EventValidator:
    """Stateful validator.

    Stream identity is (venue, instrument canonical ID, sequence_domain).
    Sequence gaps are reported, not silently repaired. Timestamps must be
    nondecreasing within that same stream. A causal child may arrive before
    its parent; it is flagged pending and rechecked when the parent appears.
    """

    def __init__(self):
        self._streams: dict[tuple, _StreamState] = {}
        self._events: dict[str, EventEnvelope] = {}
        self._pending_children: dict[str, list[EventEnvelope]] = {}

    @staticmethod
    def _stream_key(event: EventEnvelope):
        return (event.venue, event.instrument.canonical_id, event.sequence_domain)

    def validate(self, event: EventEnvelope) -> ValidationResult:
        if not isinstance(event, EventEnvelope):
            raise TypeError("event must be EventEnvelope")
        if event.event_id in self._events:
            raise EventValidationError(f"duplicate event_id: {event.event_id}")

        parent_pending = False
        if event.causal_parent_id:
            parent = self._events.get(event.causal_parent_id)
            if parent is None:
                parent_pending = True
            elif event.exchange_timestamp_ns < parent.exchange_timestamp_ns:
                raise EventValidationError("causal child timestamp precedes parent")

        key = self._stream_key(event)
        previous = self._streams.get(key)
        gap = None
        if previous is not None:
            if event.exchange_timestamp_ns < previous.exchange_timestamp_ns:
                raise EventValidationError("exchange timestamp regressed within stream")
            if event.receive_timestamp_ns < previous.receive_timestamp_ns:
                raise EventValidationError("receive timestamp regressed within stream")
            if event.sequence_number is not None and previous.sequence_number is not None:
                expected = previous.sequence_number + 1
                if event.sequence_number > expected:
                    gap = (expected, event.sequence_number - 1)
                elif event.sequence_number < expected:
                    raise EventValidationError("sequence number regressed or duplicated")
        self._streams[key] = _StreamState(
            event.exchange_timestamp_ns, event.receive_timestamp_ns,
            event.sequence_number,
        )
        self._events[event.event_id] = event

        # If this event is a parent for previously pending children, validate
        # those children now. A violation rejects the parent and rolls back its
        # insertion to avoid retaining a causally inconsistent history.
        children = self._pending_children.pop(event.event_id, [])
        invalid = [child for child in children
                   if child.exchange_timestamp_ns < event.exchange_timestamp_ns]
        if invalid:
            self._events.pop(event.event_id, None)
            self._streams[key] = previous if previous is not None else None
            if previous is None:
                self._streams.pop(key, None)
            raise EventValidationError("previously received causal child precedes this parent")
        if event.causal_parent_id and parent_pending:
            self._pending_children.setdefault(event.causal_parent_id, []).append(event)

        return ValidationResult(True, gap, parent_pending)

    def unresolved_causal_children(self) -> dict[str, tuple[str, ...]]:
        return {parent: tuple(child.event_id for child in children)
                for parent, children in self._pending_children.items()}
