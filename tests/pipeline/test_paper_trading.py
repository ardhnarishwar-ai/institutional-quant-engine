from dataclasses import replace
from decimal import Decimal
from types import SimpleNamespace

import pytest

from adapters.base.events import EventEnvelope, EventType, TradeEvent
from adapters.base.instrument import InstrumentId, InstrumentKind
from core.paper.contracts import PaperOrder, PaperOrderType
from core.paper.costs import FeeSchedule, TransactionCostModel
from core.paper.fill_models import BookLevel, MarketSnapshot
from core.paper.ledger import PaperPortfolioLedger
from core.paper.simulator import PaperSimulator
from core.signals.fusion import FusionContext
from core.risk.engine import RiskContext
from pipeline.paper_trading import PaperTradingOrchestrator


class _Analytics:
    def __init__(self, state):
        self.state = state
    def state_for(self, venue, instrument_id):
        assert venue == self.state.venue
        assert instrument_id == self.state.instrument_id
        return SimpleNamespace(snapshot=lambda: self.state)


class _Fusion:
    def evaluate(self, bundle, context):
        assert bundle.source_event_ids
        assert isinstance(context, FusionContext)
        return SimpleNamespace(accepted=False, signal=None, score=0.0)


class _Risk:
    def evaluate(self, fusion, context):
        raise AssertionError("risk must not run after fusion rejection")


def _event():
    inst = InstrumentId(
        venue="NSE", kind=InstrumentKind.NSE_SPOT, symbol="NIFTY",
        quote_currency="INR",
    )
    return EventEnvelope(
        venue="NSE", instrument=inst, event_type=EventType.TRADE,
        exchange_timestamp_ns=100, receive_timestamp_ns=110,
        sequence_number=1, sequence_domain="nse-depth",
        payload=TradeEvent(price=100.0, quantity=1.0, aggressor_side="BUY"),
        event_id="evt-1",
    )


def _state(event):
    # normalize_analytics_state consumes these named fields; this deliberately
    # tests orchestration wiring without mutating sealed analytics classes.
    unavailable = SimpleNamespace(available=False, current_vpin=None)
    return SimpleNamespace(
        venue="NSE", instrument_id=event.instrument.canonical_id,
        last_event_id=event.event_id, event_count=1, classified_trade_count=1,
        audit_sequence=1, exchange_timestamp_ns=100, receive_timestamp_ns=110,
        session_id="test", cvd=None,
        volume_profile=SimpleNamespace(poc=None, vah=None, val=None),
        vpin=None, absorption=None, gex=None, gex_as_of_timestamp_ns=None,
        dealer_positioning_assumption=None,
    )


def _orchestrator(event):
    state = _state(event)
    ledger = PaperPortfolioLedger(
        "test-account", Decimal("10000"),
        TransactionCostModel(FeeSchedule(), "INR"),
    )
    return PaperTradingOrchestrator(
        analytics=_Analytics(state),
        fusion_engines={event.instrument.canonical_id: _Fusion()},
        risk_gates={event.instrument.canonical_id: _Risk()},
        fusion_context_provider=lambda e, s: FusionContext("HEALTHY", 0, True),
        risk_context_provider=lambda e, s, f: RiskContext(
            110, "HEALTHY", 0, True, True, 0.0, 0.0, 10.0,
            0.0, 0, 0.0, 0.0, 1, 100,
        ),
        reference_price_provider=lambda e, s: None,
        paper_simulator=PaperSimulator(),
        ledger=ledger,
    ), ledger


def test_event_must_already_be_applied_to_analytics():
    event = _event()
    orch, _ = _orchestrator(event)
    wrong = replace(event, event_id="evt-mismatched")
    with pytest.raises(ValueError, match="latest accepted analytics event"):
        orch.on_accepted_event(wrong)


def test_fusion_rejection_stops_before_risk_and_order_creation():
    event = _event()
    orch, _ = _orchestrator(event)
    result = orch.on_accepted_event(event)
    assert result.status == "FUSION_REJECTED"
    assert result.risk is None
    assert result.simulation is None
    assert result.source_event_ids == ("evt-1",)


def test_duplicate_event_is_rejected():
    event = _event()
    orch, _ = _orchestrator(event)
    orch.on_accepted_event(event)
    with pytest.raises(ValueError, match="duplicate orchestrator event"):
        orch.on_accepted_event(event)


def test_snapshot_fill_is_booked_once_and_preserves_paper_only_source():
    event = _event()
    orch, ledger = _orchestrator(event)
    # Install an active order through the simulator's public submit path using
    # a minimal approved decision contract-like object and a test gate.
    decision = SimpleNamespace(
        decision_id="decision-1", instrument_id=event.instrument.canonical_id,
        venue="NSE", status="APPROVED", execution_mode="DISABLED",
        risk=SimpleNamespace(hard_veto=False, audit_sequence=1),
        source_event_ids=("evt-1",), created_at_receive_timestamp_ns=110,
        receive_timestamp_ns=110,
    )
    orch.paper_simulator.eligibility_gate.evaluate = lambda *a, **k: SimpleNamespace(
        eligible=True, reason=SimpleNamespace(value="ELIGIBLE")
    )
    order = PaperOrder(
        order_id="order-1", decision_id="decision-1", venue="NSE",
        instrument_id=event.instrument.canonical_id, direction="LONG",
        order_type=PaperOrderType.MARKETABLE_LIMIT, limit_price=Decimal("101"),
        quantity=Decimal("1"), created_exchange_timestamp_ns=100,
        created_receive_timestamp_ns=110, created_simulation_timestamp_ns=120,
        source_event_ids=("evt-1",),
    )
    assert orch.paper_simulator.submit(decision, order).accepted
    snapshot = MarketSnapshot(
        snapshot_id="snap-1", venue="NSE", instrument_id=event.instrument.canonical_id,
        bids=(BookLevel(Decimal("99"), Decimal("5")),),
        asks=(BookLevel(Decimal("100"), Decimal("5")),),
        event_ids=("evt-2",), exchange_timestamp_ns=130,
        receive_timestamp_ns=140, simulation_timestamp_ns=150,
    )
    result = orch.process_snapshot(snapshot)
    assert len(result.ledger_results) == 1
    assert result.ledger_results[0].accepted
    assert len(ledger.entries) == 1
    assert ledger.entries[0].execution_source == "PAPER_SIMULATOR"
    with pytest.raises(ValueError, match="duplicate orchestrator snapshot"):
        orch.process_snapshot(snapshot)
