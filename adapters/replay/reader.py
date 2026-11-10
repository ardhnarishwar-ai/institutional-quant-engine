"""Streaming reader for versioned canonical JSONL event journals."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable, Iterator

from pipeline.event_journal import EventJournal


class ReplayReader:
    def __init__(self, paths, include_metadata=False):
        if isinstance(paths, (str, Path)):
            paths = [paths]
        self.paths = tuple(Path(p) for p in paths)
        if not self.paths:
            raise ValueError("at least one journal path is required")
        self.include_metadata = include_metadata

    def records(self) -> Iterator[dict]:
        for path in self.paths:
            with path.open("r", encoding="utf-8") as handle:
                for line_number, line in enumerate(handle, 1):
                    if not line.strip():
                        continue
                    try:
                        record = json.loads(line)
                    except json.JSONDecodeError as exc:
                        raise ValueError(f"{path}:{line_number}: invalid JSON") from exc
                    if record.get("journal_schema_version") != 1:
                        raise ValueError(f"{path}:{line_number}: unsupported journal schema")
                    if record.get("record_type") == "event" or self.include_metadata:
                        yield record

    def events(self):
        for record in self.records():
            if record.get("record_type") == "event":
                yield EventJournal.decode_event(record)

    def merged_events(self):
        from adapters.replay.ordering import merge_event_streams
        return merge_event_streams(
            ReplayReader(path).events() for path in self.paths
        )
