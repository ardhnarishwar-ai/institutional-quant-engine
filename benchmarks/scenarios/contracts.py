"""Immutable, timestamp-neutral contracts for deterministic scenario benchmarks."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple


@dataclass(frozen=True, slots=True)
class InstrumentProfile:
    key: str
    venue: str
    instrument_id: str
    currency: str
    instrument_kind: str

    def __post_init__(self) -> None:
        for name in ("key", "venue", "instrument_id", "currency", "instrument_kind"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{name} must be a non-empty string")


@dataclass(frozen=True, slots=True)
class ScenarioTemplate:
    key: str
    expected_status: str
    description: str
    requires_options_evidence: bool = False

    def __post_init__(self) -> None:
        if not self.key or not self.expected_status or not self.description:
            raise ValueError("scenario template fields are required")


@dataclass(frozen=True, slots=True)
class ScenarioSpec:
    scenario_id: str
    template_key: str
    instrument: InstrumentProfile
    expected_status: str
    description: str
    requires_options_evidence: bool
    execution_mode: str = "DISABLED"
    repeat_count: int = 2

    def __post_init__(self) -> None:
        if not self.scenario_id or not self.template_key:
            raise ValueError("scenario identity is required")
        if self.execution_mode != "DISABLED":
            raise ValueError("benchmark execution mode must remain DISABLED")
        if self.repeat_count != 2:
            raise ValueError("Stage 5C.2 requires exactly two deterministic runs")
        if self.requires_options_evidence and self.instrument.key == "BTCUSDT":
            raise ValueError("BTCUSDT scenario cannot require fabricated options evidence")
