"""Controlled read-only shadow-session runner.

The caller supplies already-configured NSE and Binance adapters through a
local factory module. This keeps credentials, SmartAPI tokens, instrument
master data and provider snapshot acquisition outside the committed repo.
No analytics, risk, paper orders, ledger writes or execution APIs are called.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable

from adapters.execution.dead_end import DeadEndExecutionAdapter, ExecutionMode
from adapters.base.interface import AdapterState
from pipeline.event_journal import EventJournal
from pipeline.replay_verifier import verify_journals
from pipeline.shadow_capture import ReadOnlyShadowCapture


@dataclass(frozen=True, slots=True)
class SessionReport:
    status: str
    duration_seconds: float
    received: int
    persisted: int
    feed_failures: tuple[dict, ...]
    replay_event_count: int
    replay_accepted_count: int
    replay_rejected_count: int
    replay_quarantined_count: int
    journal_sha256: str
    execution_mode: str
    adapters: tuple[dict, ...]


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


async def run_session(
    adapters: Iterable,
    journal_path: str | Path,
    *,
    duration_seconds: float,
    queue_maxsize: int = 256,
    shutdown_timeout: float = 15.0,
) -> SessionReport:
    if duration_seconds <= 0:
        raise ValueError("duration_seconds must be positive")
    adapters = tuple(adapters)
    names = {getattr(a, "adapter_name", "") for a in adapters}
    if not any("BINANCE" in n.upper() for n in names):
        raise ValueError("factory must provide a Binance adapter")
    if not any("NSE" in n.upper() or "SMARTAPI" in n.upper() for n in names):
        raise ValueError("factory must provide an NSE SmartAPI adapter")

    dead_end = DeadEndExecutionAdapter()
    if dead_end.execution_mode is not ExecutionMode.DISABLED:
        raise RuntimeError("execution boundary is not disabled")

    path = Path(journal_path)
    journal = EventJournal(path, fsync=True)
    capture = ReadOnlyShadowCapture(
        adapters, journal=journal, queue_maxsize=queue_maxsize,
        shutdown_timeout=shutdown_timeout,
    )

    started_at = time.monotonic()
    await capture.start()
    try:
        await asyncio.sleep(duration_seconds)
    finally:
        await capture.stop()
    elapsed = time.monotonic() - started_at

    replay = verify_journals([path])
    failures = tuple(asdict(item) for item in capture.failures)
    states = tuple({
        "adapter": a.adapter_name,
        "state": a.state.value,
        "health": (
            {str(k): getattr(v, "status", v).__str__() for k, v in a.health_snapshot().items()}
            if callable(getattr(a, "health_snapshot", None)) else None
        ),
    } for a in adapters)

    # A valid capture requires durable records and no journal/shutdown failure.
    fatal_capture_failure = any(
        item["adapter_name"] in ("SHADOW_JOURNAL", "SHADOW_SHUTDOWN")
        for item in failures
    )
    status = "PASS" if replay.event_count == capture.stats().persisted and not fatal_capture_failure else "FAIL"
    return SessionReport(
        status=status,
        duration_seconds=elapsed,
        received=capture.stats().received,
        persisted=capture.stats().persisted,
        feed_failures=failures,
        replay_event_count=replay.event_count,
        replay_accepted_count=replay.accepted_count,
        replay_rejected_count=replay.rejected_count,
        replay_quarantined_count=replay.depth_quarantined,
        journal_sha256=_sha256(path) if path.exists() else hashlib.sha256(b"").hexdigest(),
        execution_mode="DISABLED",
        adapters=states,
    )


def load_factory(spec: str):
    """Load module:function; factory must return configured adapter instances."""
    import importlib
    if ":" not in spec:
        raise ValueError("factory must use module:function syntax")
    module_name, function_name = spec.split(":", 1)
    factory = getattr(importlib.import_module(module_name), function_name)
    adapters = factory()
    if hasattr(adapters, "__await__"):
        raise TypeError("factory must be synchronous; construct adapters before session")
    return tuple(adapters)


def main():
    spec = os.environ.get("SHADOW_ADAPTER_FACTORY")
    if not spec:
        raise SystemExit(
            "Set SHADOW_ADAPTER_FACTORY=local_module:create_adapters. "
            "Factory must construct NSE SmartAPI + Binance BTCUSDT adapters."
        )
    duration = float(os.environ.get("SHADOW_DURATION_SECONDS", "60"))
    queue_size = int(os.environ.get("SHADOW_QUEUE_MAXSIZE", "256"))
    journal = os.environ.get("SHADOW_JOURNAL", "reports/live_shadow_session.jsonl")
    report_path = Path(os.environ.get("SHADOW_REPORT", "reports/stage5d4_live_shadow_report.json"))
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report = asyncio.run(run_session(
        load_factory(spec), journal,
        duration_seconds=duration, queue_maxsize=queue_size,
    ))
    report_path.write_text(json.dumps(asdict(report), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(asdict(report), indent=2, sort_keys=True))
    print(f"REPORT_SHA256={_sha256(report_path)}")
    if report.status != "PASS":
        raise SystemExit(2)


if __name__ == "__main__":
    main()
