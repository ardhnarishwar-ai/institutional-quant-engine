"""Explicit no-route execution adapter for observational-only deployments.

This object is a terminal boundary, not a broker abstraction. It has no
transport, credentials, URL, socket, SDK, or delegation hook. Every attempted
execution operation fails locally and synchronously.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import NoReturn


class ExecutionDisabledError(RuntimeError):
    """Raised whenever a caller attempts to use the disabled execution boundary."""


class ExecutionMode(str, Enum):
    DISABLED = "DISABLED"


@dataclass(frozen=True, slots=True)
class DeadEndExecutionAdapter:
    """A deliberately non-functional execution endpoint."""

    execution_mode: ExecutionMode = ExecutionMode.DISABLED

    def __post_init__(self) -> None:
        if self.execution_mode is not ExecutionMode.DISABLED:
            raise ValueError("execution mode is permanently DISABLED")

    @staticmethod
    def _reject(operation: str) -> NoReturn:
        raise ExecutionDisabledError(
            f"{operation} is unavailable: execution mode is permanently DISABLED"
        )

    def submit_order(self, *_args: object, **_kwargs: object) -> NoReturn:
        self._reject("submit_order")

    def cancel_order(self, *_args: object, **_kwargs: object) -> NoReturn:
        self._reject("cancel_order")

    def replace_order(self, *_args: object, **_kwargs: object) -> NoReturn:
        self._reject("replace_order")

    def close_position(self, *_args: object, **_kwargs: object) -> NoReturn:
        self._reject("close_position")
