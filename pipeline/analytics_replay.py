"""Deterministic journal replay into the unified microstructure analytics state.

Canonical EventEnvelope records are replayed in journal order. Side-unknown
trade observations and session boundaries are explicit journal metadata records:
they are not fabricated into canonical TradeEvent objects.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from adapters.replay.reader import ReplayReader
from core.microstructure.analytics_state import (
    AnalyticsRegistry,
    AnalyticsState,
    AnalyticsStateError,
)


@dataclass(frozen=True, slots=True)
class AnalyticsReplayIssue:
    record_index: int
    record_type: str
    event_id: Optional[str]
    reason: str


@dataclass(frozen=True, slots=True)
class AnalyticsReplayResult:
    record_count: int
    accepted_event_count: int
    rejected_record_count: int
    unknown_trade_count: int
    session_boundary_count: int
    snapshots: tuple[AnalyticsState, ...]
    issues: tuple[AnalyticsReplayIssue, ...]


def replay_analytics_journals(paths, registry: AnalyticsRegistry, *, fail_fast: bool = False):
    """Replay one or more JSONL journals in supplied file/record order.

    Supported metadata kinds:
      * ``UNKNOWN_TRADE``: venue, instrument_id, event_id, price, quantity,
        exchange_timestamp_ns (nullable), receive_timestamp_ns.
      * ``SESSION_BOUNDARY``: venue, instrument_id, session_id.

    Unknown-side observations stay outside canonical EventEnvelope/TradeEvent.
    Timestamp and duplicate validation are delegated to the same live analytics
    state used by the application. Rejected records do not halt replay unless
    fail_fast=True.
    """
    if not isinstance(registry, AnalyticsRegistry):
        raise TypeError("registry must be AnalyticsRegistry")

    reader = ReplayReader(paths, include_metadata=True)
    accepted = rejected = unknown = boundaries = total = 0
    issues = []

    for index, record in enumerate(reader.records(), start=1):
        total += 1
        record_type = record.get("record_type")
        event_id = None
        try:
            if record_type == "event":
                from pipeline.event_journal import EventJournal
                event = EventJournal.decode_event(record)
                event_id = event.event_id
                registry.update(event)
                accepted += 1
                continue

            if record_type != "metadata":
                raise ValueError(f"unsupported record_type: {record_type!r}")
            metadata = record.get("metadata")
            if not isinstance(metadata, dict):
                raise ValueError("metadata record must contain an object")
            kind = metadata.get("kind")

            if kind == "UNKNOWN_TRADE":
                event_id = metadata.get("event_id")
                state = registry.state_for(metadata["venue"], metadata["instrument_id"])
                state.record_unclassified_trade(
                    event_id=event_id,
                    price=metadata["price"],
                    quantity=metadata["quantity"],
                    exchange_timestamp_ns=metadata.get("exchange_timestamp_ns"),
                    receive_timestamp_ns=metadata["receive_timestamp_ns"],
                )
                unknown += 1
            elif kind == "SESSION_BOUNDARY":
                state = registry.state_for(metadata["venue"], metadata["instrument_id"])
                state.start_session(metadata["session_id"])
                boundaries += 1
            else:
                raise ValueError(f"unsupported analytics metadata kind: {kind!r}")

        except (AnalyticsStateError, ValueError, KeyError, TypeError) as exc:
            rejected += 1
            issue = AnalyticsReplayIssue(index, str(record_type), event_id, f"{type(exc).__name__}: {exc}")
            issues.append(issue)
            if fail_fast:
                raise

    return AnalyticsReplayResult(
        record_count=total,
        accepted_event_count=accepted,
        rejected_record_count=rejected,
        unknown_trade_count=unknown,
        session_boundary_count=boundaries,
        snapshots=registry.snapshots(),
        issues=tuple(issues),
    )
