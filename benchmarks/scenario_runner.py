"""Dual-run scenario harness with canonical SHA-256 reproducibility digests.

The executor is an explicit integration seam. Production callers should bind
it to the Stage 5C.1 PaperTradingOrchestrator and real journal/replay fixtures.
This module never fabricates exchange, receive, or simulation timestamps.
"""
from __future__ import annotations

import dataclasses
import hashlib
import json
import math
from decimal import Decimal
from enum import Enum
from typing import Any, Callable, Mapping

from benchmarks.scenarios.contracts import ScenarioSpec


def canonicalize(value: Any) -> Any:
    """Convert supported result values into stable JSON-compatible objects."""
    if dataclasses.is_dataclass(value):
        return {
            field.name: canonicalize(getattr(value, field.name))
            for field in dataclasses.fields(value)
        }
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, Mapping):
        return {
            str(key): canonicalize(value[key])
            for key in sorted(value, key=lambda item: str(item))
        }
    if isinstance(value, (tuple, list)):
        return [canonicalize(item) for item in value]
    if isinstance(value, (set, frozenset)):
        normalized = [canonicalize(item) for item in value]
        return sorted(normalized, key=lambda item: json.dumps(item, sort_keys=True))
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("non-finite floats cannot enter deterministic report")
        return format(value, ".17g")
    if value is None or isinstance(value, (str, int, bool)):
        return value
    raise TypeError(f"unsupported deterministic result type: {type(value).__name__}")


def result_digest(result: Any) -> str:
    encoded = json.dumps(
        canonicalize(result), sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


class DeterminismError(AssertionError):
    pass


class ScenarioBenchmarkRunner:
    """Execute each scenario twice using a caller-provided fresh-state executor.

    executor(spec, run_index) must construct fresh per-run state and return a
    deterministic observation. It must not reuse mutable simulator/ledger
    instances between run_index 1 and 2.
    """

    def __init__(self, executor: Callable[[ScenarioSpec, int], Any]) -> None:
        if not callable(executor):
            raise TypeError("executor must be callable")
        self.executor = executor

    def run_one(self, spec: ScenarioSpec) -> dict[str, Any]:
        if not isinstance(spec, ScenarioSpec):
            raise TypeError("ScenarioSpec required")
        first = self.executor(spec, 1)
        second = self.executor(spec, 2)
        first_digest = result_digest(first)
        second_digest = result_digest(second)
        if first_digest != second_digest:
            raise DeterminismError(
                f"non-deterministic scenario {spec.scenario_id}: "
                f"{first_digest} != {second_digest}"
            )
        return {
            "scenario_id": spec.scenario_id,
            "template_key": spec.template_key,
            "instrument": spec.instrument.key,
            "currency": spec.instrument.currency,
            "expected_status": spec.expected_status,
            "execution_mode": spec.execution_mode,
            "repeat_count": 2,
            "run_1_digest": first_digest,
            "run_2_digest": second_digest,
            "result_digest": first_digest,
            "deterministic": True,
            "observation": canonicalize(first),
        }

    def run_matrix(self, specs: tuple[ScenarioSpec, ...]) -> dict[str, Any]:
        rows = [self.run_one(spec) for spec in specs]
        report = {
            "stage": "5C.2",
            "execution_mode": "DISABLED",
            "template_count": 12,
            "instrument_count": 4,
            "scenario_count": len(rows),
            "repeat_count": 2,
            "determinism": "PASS" if all(row["deterministic"] for row in rows) else "FAIL",
            "instrument_isolation": "REQUIRES_EXECUTOR_ASSERTION",
            "ledger_currency_isolation": "REQUIRES_EXECUTOR_ASSERTION",
            "scenarios": rows,
        }
        report["report_digest"] = result_digest({
            key: value for key, value in report.items() if key != "report_digest"
        })
        return report
