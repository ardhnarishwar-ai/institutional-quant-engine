"""Paper-only simulation contracts. No broker execution is implemented here."""

from .contracts import (
    ExecutionSource,
    PaperAccount,
    PaperFill,
    PaperOrder,
    PaperOrderStatus,
    PaperOrderType,
    PaperPosition,
)
from .eligibility import (
    EligibilityReason,
    EligibilityResult,
    PaperEligibilityGate,
)
from .policies import PaperInstrumentPolicy, default_paper_policies

__all__ = [
    "ExecutionSource", "PaperAccount", "PaperFill", "PaperOrder",
    "PaperOrderStatus", "PaperOrderType", "PaperPosition",
    "EligibilityReason", "EligibilityResult", "PaperEligibilityGate",
    "PaperInstrumentPolicy", "default_paper_policies",
]
