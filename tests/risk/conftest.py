import pytest
from core.signals.contracts import (
    ConfidenceMetrics, ExecutionMode, SignalDirection, SignalEvent, SignalProvenance,
)
from core.signals.fusion import FusionResult
from core.risk.engine import RiskContext
from core.risk.policy import DEFAULT_RISK_POLICIES


NIFTY = "NSE:NSE_SPOT:NIFTY:INR"


@pytest.fixture
def policy():
    return DEFAULT_RISK_POLICIES[NIFTY]


@pytest.fixture
def signal():
    return SignalEvent(
        signal_id="fusion:NIFTY:1", venue="NSE", instrument_id=NIFTY,
        direction=SignalDirection.LONG,
        confidence=ConfidenceMetrics(0.8, 0.9, 12),
        provenance=SignalProvenance(("evt-1", "evt-2"), 1_000_000_000, 1_000_000_100, 7),
        created_at_receive_timestamp_ns=1_000_000_100,
        execution_mode=ExecutionMode.DISABLED,
    )


@pytest.fixture
def fusion(signal):
    return FusionResult(True, signal.direction, signal, 0.75, 0.9, 3, (), (), ("FUSION_V1",))


@pytest.fixture
def context():
    return RiskContext(
        now_receive_timestamp_ns=1_000_000_200, feed_status="HEALTHY",
        feed_age_ns=100, book_synchronized=True, history_ready=True,
        market_risk=0.1, liquidity_spread_ticks=1.0, liquidity_depth_units=5.0,
        vpin_risk=0.1, gex_age_ns=1000, exposure_utilization=0.1,
        daily_loss_utilization=0.05, audit_sequence=8,
        exchange_reference_timestamp_ns=1_000_000_000,
    )
