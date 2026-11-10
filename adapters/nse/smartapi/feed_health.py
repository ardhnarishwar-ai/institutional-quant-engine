"""Observable feed health with independent event clocks and sequence domains."""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional


class FeedHealthError(ValueError):
    pass


class FeedStatus(str, Enum):
    STARTING = "STARTING"
    HEALTHY = "HEALTHY"
    STALE = "STALE"
    DESYNCHRONIZED = "DESYNCHRONIZED"
    DISCONNECTED = "DISCONNECTED"


@dataclass(frozen=True, slots=True)
class FeedHealthSnapshot:
    status: FeedStatus
    last_receive_timestamp_ns: Optional[int]
    last_exchange_timestamp_ns: Optional[int]
    last_sequence_number: Optional[int]
    latency_ns: Optional[int]
    stale_for_ns: Optional[int]
    sequence_gap: Optional[tuple[int, int]]
    packets_seen: int
    clock_skew: bool
    flags: tuple[str, ...]


@dataclass
class _Stream:
    last_receive_ns: Optional[int] = None
    last_exchange_ns: Optional[int] = None
    last_sequence: Optional[int] = None
    last_monotonic_ns: Optional[int] = None
    latency_ns: Optional[int] = None
    gap: Optional[tuple[int, int]] = None
    packets: int = 0
    status: FeedStatus = FeedStatus.STARTING
    clock_skew: bool = False


class FeedHealthMonitor:
    """Tracks each venue/instrument/sequence-domain independently.

    receive_timestamp_ns and exchange_timestamp_ns are retained as supplied.
    Staleness uses an injected monotonic clock, never either event timestamp.
    Latency is signed (receive - exchange); negative values are exposed as
    clock skew rather than clamped or silently corrected.
    """
    def __init__(self, *, stale_after_ns: int = 3_000_000_000):
        if isinstance(stale_after_ns, bool) or not isinstance(stale_after_ns, int) or stale_after_ns <= 0:
            raise ValueError("stale_after_ns must be a positive integer")
        self.stale_after_ns = stale_after_ns
        self._streams: dict[tuple, _Stream] = {}

    @staticmethod
    def _key(event):
        return (event.venue, event.instrument.canonical_id, event.sequence_domain)

    def observe(self, event, *, monotonic_ns: int) -> FeedHealthSnapshot:
        if isinstance(monotonic_ns, bool) or not isinstance(monotonic_ns, int) or monotonic_ns < 0:
            raise FeedHealthError("monotonic_ns must be a non-negative integer")
        key = self._key(event)
        state = self._streams.setdefault(key, _Stream())
        if state.last_monotonic_ns is not None and monotonic_ns < state.last_monotonic_ns:
            raise FeedHealthError("monotonic clock regressed")
        if state.last_receive_ns is not None and event.receive_timestamp_ns < state.last_receive_ns:
            raise FeedHealthError("receive timestamp regressed")
        if (event.exchange_timestamp_ns is not None and state.last_exchange_ns is not None
                and event.exchange_timestamp_ns < state.last_exchange_ns):
            raise FeedHealthError("exchange timestamp regressed")

        gap = None
        if event.sequence_number is not None and state.last_sequence is not None:
            if event.sequence_number <= state.last_sequence:
                state.status = FeedStatus.DESYNCHRONIZED
                state.gap = (event.sequence_number, state.last_sequence)
            elif event.sequence_number > state.last_sequence + 1:
                gap = (state.last_sequence + 1, event.sequence_number - 1)
                state.status = FeedStatus.DESYNCHRONIZED
                state.gap = gap
        if event.sequence_number is not None and (
            state.last_sequence is None or event.sequence_number > state.last_sequence
        ):
            state.last_sequence = event.sequence_number

        state.last_receive_ns = event.receive_timestamp_ns
        state.last_exchange_ns = event.exchange_timestamp_ns
        state.last_monotonic_ns = monotonic_ns
        state.packets += 1
        state.latency_ns = (
            event.receive_timestamp_ns - event.exchange_timestamp_ns
            if event.exchange_timestamp_ns is not None else None
        )
        state.clock_skew = state.latency_ns is not None and state.latency_ns < 0
        if state.status != FeedStatus.DESYNCHRONIZED:
            state.status = FeedStatus.HEALTHY
        return self.snapshot_for_key(key, monotonic_ns=monotonic_ns, new_gap=gap)

    def snapshot_for_key(self, key, *, monotonic_ns: int, new_gap=None):
        state = self._streams.get(key)
        if state is None:
            return FeedHealthSnapshot(FeedStatus.STARTING, None, None, None, None,
                                      None, None, 0, False, ("NO_PACKETS",))
        stale_for = max(0, monotonic_ns - state.last_monotonic_ns) if state.last_monotonic_ns is not None else None
        status = state.status
        flags = []
        if status == FeedStatus.DESYNCHRONIZED:
            flags.append("SEQUENCE_GAP_OR_REGRESSION")
        if stale_for is not None and stale_for > self.stale_after_ns:
            # Staleness is the current transport condition. Keep sequence
            # desynchronization visible independently in flags and gap history.
            if status != FeedStatus.DISCONNECTED:
                status = FeedStatus.STALE
            flags.append("RECEIVE_STALE")
        if state.clock_skew:
            flags.append("NEGATIVE_MEASURED_LATENCY_CLOCK_SKEW")
        if state.last_exchange_ns is None:
            flags.append("EXCHANGE_TIMESTAMP_UNVERIFIED")
        return FeedHealthSnapshot(
            status, state.last_receive_ns, state.last_exchange_ns,
            state.last_sequence, state.latency_ns, stale_for,
            state.gap if new_gap is None else new_gap, state.packets,
            state.clock_skew, tuple(flags),
        )

    def snapshot(self, venue, instrument_id, sequence_domain, *, monotonic_ns: int):
        return self.snapshot_for_key(
            (venue.upper(), instrument_id, sequence_domain), monotonic_ns=monotonic_ns
        )

    def mark_disconnected(self):
        for state in self._streams.values():
            state.status = FeedStatus.DISCONNECTED

    def mark_desynchronized(self, venue, instrument_id, sequence_domain):
        key = (venue.upper(), instrument_id, sequence_domain)
        state = self._streams.setdefault(key, _Stream())
        state.status = FeedStatus.DESYNCHRONIZED

    def recover_with_snapshot(self, venue, instrument_id, sequence_domain):
        key = (venue.upper(), instrument_id, sequence_domain)
        state = self._streams.setdefault(key, _Stream())
        state.status = FeedStatus.HEALTHY
        state.gap = None
        # A snapshot without provider sequence must not invent a sequence baseline.
