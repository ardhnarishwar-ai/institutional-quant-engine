"""Execution boundary components. Live order routing is intentionally absent."""
from adapters.execution.dead_end import (
    DeadEndExecutionAdapter,
    ExecutionDisabledError,
)

__all__ = ["DeadEndExecutionAdapter", "ExecutionDisabledError"]
