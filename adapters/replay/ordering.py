"""Stable deterministic interleaving of already time-ordered event streams."""
from __future__ import annotations

import heapq
from itertools import count


def merge_event_streams(streams):
    """Heap-merge sorted iterables by exchange time and stable tie breakers.

    Each input stream must be nondecreasing by exchange_timestamp_ns. Ties are
    resolved by venue, instrument ID, sequence domain, sequence number, event
    ID, then source-stream ordinal. No local wall clock participates.
    """
    heap = []
    serial = count()

    def key(event, source_index):
        return (
            event.exchange_timestamp_ns,
            event.venue,
            event.instrument.canonical_id,
            event.sequence_domain or "",
            -1 if event.sequence_number is None else event.sequence_number,
            event.event_id,
            source_index,
        )

    iterators = [iter(stream) for stream in streams]
    previous = [None] * len(iterators)
    for i, iterator in enumerate(iterators):
        try:
            event = next(iterator)
        except StopIteration:
            continue
        previous[i] = event.exchange_timestamp_ns
        heapq.heappush(heap, (key(event, i), next(serial), event, i))

    while heap:
        _, _, event, i = heapq.heappop(heap)
        yield event
        try:
            nxt = next(iterators[i])
        except StopIteration:
            continue
        if nxt.exchange_timestamp_ns < previous[i]:
            raise ValueError(f"input stream {i} is not sorted by exchange timestamp")
        previous[i] = nxt.exchange_timestamp_ns
        heapq.heappush(heap, (key(nxt, i), next(serial), nxt, i))
