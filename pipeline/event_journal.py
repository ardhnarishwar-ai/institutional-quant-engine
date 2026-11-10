"""Append-only JSONL journal preserving original exchange and receive times."""
from __future__ import annotations

from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from enum import Enum
import json
import os
from pathlib import Path
from threading import Lock

from adapters.base.events import (
    DepthDeltaEvent, DepthSnapshotEvent, DepthSnapshotLevel, EventEnvelope,
    EventType, OptionMetricEvent, QuoteEvent, TradeEvent,
)
from adapters.base.instrument import InstrumentId, InstrumentKind

JOURNAL_SCHEMA_VERSION = 1


def _jsonable(value):
    if isinstance(value, Enum):
        return value.value
    if is_dataclass(value):
        return {k: _jsonable(v) for k, v in asdict(value).items()}
    if isinstance(value, tuple):
        return [_jsonable(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    return value


class EventJournal:
    """Append-only JSON Lines journal; timestamps are never rewritten."""

    def __init__(self, path, fsync=False):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.fsync = fsync
        self._lock = Lock()

    def append(self, event: EventEnvelope):
        if not isinstance(event, EventEnvelope):
            raise TypeError("journal accepts EventEnvelope only")
        record = {
            "record_type": "event",
            "journal_schema_version": JOURNAL_SCHEMA_VERSION,
            "event": {
                "venue": event.venue,
                "instrument": _jsonable(event.instrument),
                "event_type": event.event_type.value,
                "exchange_timestamp_ns": event.exchange_timestamp_ns,
                "receive_timestamp_ns": event.receive_timestamp_ns,
                "sequence_number": event.sequence_number,
                "sequence_domain": event.sequence_domain,
                "payload": _jsonable(event.payload),
                "event_id": event.event_id,
                "schema_version": event.schema_version,
                "causal_parent_id": event.causal_parent_id,
            },
        }
        self._append_record(record)

    def append_metadata(self, metadata: dict):
        record = {
            "record_type": "metadata",
            "journal_schema_version": JOURNAL_SCHEMA_VERSION,
            "metadata": _jsonable(metadata),
        }
        self._append_record(record)

    def _append_record(self, record):
        line = json.dumps(record, separators=(",", ":"), sort_keys=True, allow_nan=False)
        with self._lock:
            with self.path.open("a", encoding="utf-8") as handle:
                handle.write(line + "\n")
                handle.flush()
                if self.fsync:
                    os.fsync(handle.fileno())

    @staticmethod
    def decode_event(record):
        if record.get("record_type") != "event":
            raise ValueError("record is not an event")
        if record.get("journal_schema_version") != JOURNAL_SCHEMA_VERSION:
            raise ValueError("unsupported journal schema version")
        raw = record["event"]
        inst = raw["instrument"]
        instrument = InstrumentId(
            venue=inst["venue"], kind=InstrumentKind(inst["kind"]),
            symbol=inst["symbol"], quote_currency=inst["quote_currency"],
            expiry=inst.get("expiry"), strike=inst.get("strike"),
            option_type=inst.get("option_type"),
        )
        typ = EventType(raw["event_type"])
        payload_raw = raw["payload"]
        if typ == EventType.TRADE:
            payload = TradeEvent(**payload_raw)
        elif typ == EventType.QUOTE:
            payload = QuoteEvent(**payload_raw)
        elif typ == EventType.DEPTH_DELTA:
            payload = DepthDeltaEvent(**payload_raw)
        elif typ == EventType.DEPTH_SNAPSHOT:
            payload = DepthSnapshotEvent(
                bids=tuple(DepthSnapshotLevel(**x) for x in payload_raw["bids"]),
                asks=tuple(DepthSnapshotLevel(**x) for x in payload_raw["asks"]),
                snapshot_sequence=payload_raw["snapshot_sequence"],
            )
        elif typ == EventType.OPTION_METRIC:
            payload = OptionMetricEvent(**payload_raw)
        else:
            raise ValueError(f"unsupported event type {typ}")
        return EventEnvelope(
            venue=raw["venue"], instrument=instrument, event_type=typ,
            exchange_timestamp_ns=raw["exchange_timestamp_ns"],
            receive_timestamp_ns=raw["receive_timestamp_ns"],
            sequence_number=raw["sequence_number"],
            sequence_domain=raw["sequence_domain"], payload=payload,
            event_id=raw["event_id"], schema_version=raw["schema_version"],
            causal_parent_id=raw.get("causal_parent_id"),
        )
