"""Fixed-size, deterministic stress workloads using sealed public contracts."""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal as D

from core.paper.contracts import (
    PaperOrder, PaperOrderType, PaperFill, ExecutionSource,
)
from core.paper.costs import FeeSchedule, TransactionCostModel
from core.paper.fill_models import BookLevel, MarketSnapshot
from core.paper.ledger import PaperPortfolioLedger, AccountingMode
from core.paper.simulator import PaperSimulator
from core.risk.contracts import DecisionEvent, DecisionStatus, RiskAssessment, RiskSeverity
from core.microstructure.analytics_state import AnalyticsRegistry
from pipeline.paper_trading import PaperTradingOrchestrator


@dataclass(frozen=True)
class InstrumentProfile:
    key: str
    venue: str
    instrument_id: str
    currency: str
    price: D
    tick: D


PROFILES = (
    InstrumentProfile("NIFTY", "NSE", "NSE:NSE_SPOT:NIFTY:INR", "INR", D("25000"), D("0.05")),
    InstrumentProfile("BANKNIFTY", "NSE", "NSE:NSE_SPOT:BANKNIFTY:INR", "INR", D("52000"), D("0.05")),
    InstrumentProfile("FINNIFTY", "NSE", "NSE:NSE_SPOT:FINNIFTY:INR", "INR", D("23000"), D("0.05")),
    InstrumentProfile("BTCUSDT", "BINANCE", "BINANCE:CRYPTO_PERPETUAL:BTCUSDT:USDT", "USDT", D("60000"), D("0.1")),
)


def make_decision(profile: InstrumentProfile, index: int) -> DecisionEvent:
    assessment = RiskAssessment(
        instrument_id=profile.instrument_id,
        risk_score=0.1,
        risk_band=RiskSeverity.LOW,
        checks=(),
        hard_veto=False,
        reasons=(),
        policy_version="stress-risk-v1",
        audit_sequence=index + 1,
    )
    return DecisionEvent(
        decision_id=f"stress-decision-{profile.key}-{index:04d}",
        signal_id=f"stress-signal-{profile.key}-{index:04d}",
        venue=profile.venue,
        instrument_id=profile.instrument_id,
        status=DecisionStatus.APPROVED,
        direction="LONG",
        risk=assessment,
        source_event_ids=(f"journal-decision-{profile.key}-{index:04d}",),
        exchange_timestamp_ns=None,
        receive_timestamp_ns=10_000 + index,
        created_at_receive_timestamp_ns=10_001 + index,
        execution_mode="DISABLED",
    )


def make_order(profile: InstrumentProfile, index: int, quantity: D = D("3")) -> PaperOrder:
    return PaperOrder(
        order_id=f"stress-order-{profile.key}-{index:04d}",
        decision_id=f"stress-decision-{profile.key}-{index:04d}",
        venue=profile.venue,
        instrument_id=profile.instrument_id,
        direction="LONG",
        order_type=PaperOrderType.MARKETABLE_LIMIT,
        limit_price=profile.price + profile.tick * D("2"),
        quantity=quantity,
        created_exchange_timestamp_ns=None,
        created_receive_timestamp_ns=10_000 + index,
        created_simulation_timestamp_ns=20_000 + index,
        source_event_ids=(f"journal-order-{profile.key}-{index:04d}",),
    )


def make_snapshot(profile: InstrumentProfile, snapshot_index: int, displayed_qty: D) -> MarketSnapshot:
    return MarketSnapshot(
        snapshot_id=f"stress-snapshot-{profile.key}-{snapshot_index:04d}",
        venue=profile.venue,
        instrument_id=profile.instrument_id,
        bids=(BookLevel(profile.price - profile.tick, D("100000")),),
        asks=(BookLevel(profile.price, displayed_qty),),
        event_ids=(f"market-trade-{profile.key}-{snapshot_index:04d}",
                   f"market-depth-{profile.key}-{snapshot_index:04d}"),
        exchange_timestamp_ns=30_000 + snapshot_index,
        receive_timestamp_ns=40_000 + snapshot_index,
        simulation_timestamp_ns=50_000 + snapshot_index,
    )


def make_stack(profile: InstrumentProfile):
    """Fresh orchestrator, simulator and one-currency ledger per invocation."""
    analytics = AnalyticsRegistry(tick_size_by_instrument={profile.instrument_id: float(profile.tick)})
    simulator = PaperSimulator()
    costs = TransactionCostModel(FeeSchedule(), profile.currency)
    ledger = PaperPortfolioLedger(
        account_id=f"stress-account-{profile.key}",
        initial_cash=D("1000000000"),
        cost_model=costs,
        accounting_mode=AccountingMode.CASH,
    )
    orchestrator = PaperTradingOrchestrator(
        analytics=analytics,
        fusion_engines={},
        risk_gates={},
        fusion_context_provider=lambda event, state: None,
        risk_context_provider=lambda event, state, fusion: None,
        reference_price_provider=lambda event, state: None,
        paper_simulator=simulator,
        ledger=ledger,
    )
    return orchestrator, simulator, ledger


def submit_batch(profile: InstrumentProfile, simulator: PaperSimulator, count: int):
    accepted = 0
    for i in range(count):
        decision = make_decision(profile, i)
        result = simulator.submit(decision, make_order(profile, i))
        if result.accepted:
            accepted += 1
        else:
            raise AssertionError(f"order {i} rejected: {result.reason}")
    return accepted


def run_profile_workload(profile: InstrumentProfile, order_count: int = 32):
    orchestrator, simulator, ledger = make_stack(profile)
    accepted = submit_batch(profile, simulator, order_count)
    # First snapshot produces bounded partial fills; second completes each order.
    first = orchestrator.process_snapshot(make_snapshot(profile, 1, D("1")))
    second = orchestrator.process_snapshot(make_snapshot(profile, 2, D("10")))
    portfolio = ledger.mark_to_market({})
    if len(ledger.entries) != len(simulator.fills):
        raise AssertionError("ledger entries and simulator fills diverged")
    if len({entry.fill_id for entry in ledger.entries}) != len(ledger.entries):
        raise AssertionError("duplicate accounting entry detected")
    if ledger.cost_model.currency != profile.currency or portfolio.currency != profile.currency:
        raise AssertionError("currency isolation violation")
    if any(position.currency != profile.currency for position in ledger.positions):
        raise AssertionError("position currency contamination")
    if simulator.EXECUTION_MODE != "DISABLED":
        raise AssertionError("paper simulator execution mode is not DISABLED")
    return {
        "instrument": profile.key,
        "venue": profile.venue,
        "instrument_id": profile.instrument_id,
        "currency": profile.currency,
        "accepted_orders": accepted,
        "fills": len(simulator.fills),
        "ledger_entries": len(ledger.entries),
        "ledger_sequence": portfolio.ledger_sequence,
        "cash": str(portfolio.cash),
        "realized_pnl": str(portfolio.realized_pnl),
        "unrealized_pnl": str(portfolio.unrealized_pnl),
        "execution_mode": simulator.EXECUTION_MODE,
        "first_snapshot_results": len(first.simulation_results),
        "second_snapshot_results": len(second.simulation_results),
    }


def make_synthetic_paper_fill(profile: InstrumentProfile, index: int) -> PaperFill:
    """Valid paper-only accounting fixture; no exchange execution is implied."""
    return PaperFill(
        fill_id=f"ledger-stress-{profile.key}-{index:05d}",
        order_id=f"ledger-order-{profile.key}-{index:05d}",
        venue=profile.venue,
        instrument_id=profile.instrument_id,
        side="BUY" if index % 2 == 0 else "SELL",
        price=profile.price + profile.tick * D(index % 3),
        quantity=D("0.01") if profile.key == "BTCUSDT" else D("1"),
        execution_source=ExecutionSource.PAPER_SIMULATOR,
        market_evidence_event_ids=(f"evidence-trade-{index}", f"evidence-depth-{index}"),
        fill_model_version="stage5c4.3-ledger-stress-v1",
        exchange_timestamp_ns=None if index % 5 == 0 else 60_000 + index,
        receive_timestamp_ns=70_000 + index,
        simulation_timestamp_ns=80_000 + index,
    )
