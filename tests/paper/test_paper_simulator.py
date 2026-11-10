from dataclasses import replace
from decimal import Decimal

from core.paper.contracts import PaperOrder, PaperOrderStatus, PaperOrderType
from core.paper.eligibility import EligibilityReason
from core.paper.fill_models import BookLevel, MarketSnapshot
from core.paper.simulator import PaperSimulator, SimulationReason

from core.risk.contracts import DecisionEvent, DecisionStatus, RiskAssessment, RiskSeverity


def approved_decision():
    risk = RiskAssessment(
        instrument_id="NSE:NSE_SPOT:NIFTY:INR",
        risk_score=0.1,
        risk_band=RiskSeverity.LOW,
        checks=(),
        hard_veto=False,
        reasons=(),
        policy_version="risk-v1",
        audit_sequence=1,
    )
    return DecisionEvent(
        decision_id="d-1", signal_id="s-1", venue="NSE",
        instrument_id="NSE:NSE_SPOT:NIFTY:INR",
        status=DecisionStatus.APPROVED, direction="LONG",
        risk=risk, source_event_ids=("journal-event-1",),
        exchange_timestamp_ns=None, receive_timestamp_ns=100,
        created_at_receive_timestamp_ns=101, execution_mode="DISABLED",
    )




def order(**kw):
    values = dict(
        order_id="paper-order-1", decision_id="d-1", venue="NSE",
        instrument_id="NSE:NSE_SPOT:NIFTY:INR", direction="LONG",
        order_type=PaperOrderType.LIMIT, limit_price=Decimal("101"),
        quantity=Decimal("3"), created_exchange_timestamp_ns=None,
        created_receive_timestamp_ns=100, created_simulation_timestamp_ns=101,
        source_event_ids=("decision-event-1",),
    )
    values.update(kw)
    return PaperOrder(**values)


def snapshot(snapshot_id="s1", *, ask_qty="2", ask="101", exchange_ns=None, **kw):
    values = dict(
        snapshot_id=snapshot_id, venue="NSE",
        instrument_id="NSE:NSE_SPOT:NIFTY:INR",
        bids=(BookLevel("100", "10"),), asks=(BookLevel(ask, ask_qty),),
        event_ids=(f"quote-{snapshot_id}", f"depth-{snapshot_id}"),
        exchange_timestamp_ns=exchange_ns, receive_timestamp_ns=200,
        simulation_timestamp_ns=201,
    )
    values.update(kw)
    return MarketSnapshot(**values)


def setup_sim():
    sim = PaperSimulator()
    result = sim.submit(approved_decision(), order())
    assert result.accepted and result.reason is SimulationReason.ORDER_ACCEPTED
    return sim


def test_partial_then_complete_fill_and_full_audit_fields():
    sim = setup_sim()
    first = sim.process_snapshot(snapshot("s1"))[0]
    assert first.reason is SimulationReason.PARTIALLY_FILLED
    assert first.order.filled_quantity == Decimal("2")
    assert first.fill.execution_source.value == "PAPER_SIMULATOR"
    assert first.fill.market_evidence_event_ids == ("quote-s1", "depth-s1")
    assert first.fill.fill_model_version == "top-of-book-marketable-limit-v1"
    assert first.fill.exchange_timestamp_ns is None
    assert first.fill.receive_timestamp_ns == 200
    assert first.fill.simulation_timestamp_ns == 201

    second = sim.process_snapshot(snapshot("s2", ask_qty="5", exchange_ns=202))[0]
    assert second.reason is SimulationReason.FILLED
    assert second.order.status is PaperOrderStatus.FILLED
    assert second.order.filled_quantity == Decimal("3")
    assert second.fill.exchange_timestamp_ns == 202
    assert len(sim.fills) == 2


def test_duplicate_snapshot_cannot_double_fill():
    sim = setup_sim()
    first = sim.process_snapshot(snapshot("same"))[0]
    second = sim.process_snapshot(snapshot("same"))[0]
    assert first.fill is not None
    assert second.reason is SimulationReason.DUPLICATE_SNAPSHOT
    assert len(sim.fills) == 1
    assert sim.orders[0].filled_quantity == Decimal("2")


def test_non_marketable_order_remains_active():
    sim = setup_sim()
    result = sim.process_snapshot(snapshot("not-crossing", ask="102"))[0]
    assert result.reason is SimulationReason.NOT_MARKETABLE
    assert result.order.status is PaperOrderStatus.ACTIVE
    assert not sim.fills


def test_unhealthy_or_desynchronized_snapshot_never_fills():
    sim = setup_sim()
    result = sim.process_snapshot(snapshot("stale", feed_healthy=False))[0]
    assert result.reason is SimulationReason.FEED_UNHEALTHY
    result = sim.process_snapshot(snapshot("gap", book_synchronized=False))[0]
    assert result.reason is SimulationReason.BOOK_DESYNCHRONIZED
    assert not sim.fills


def test_decision_and_order_identity_must_match():
    sim = PaperSimulator()
    wrong = replace(order(), instrument_id="BANKNIFTY")
    result = sim.submit(approved_decision(), wrong)
    assert not result.accepted
    assert result.reason is SimulationReason.DECISION_ORDER_MISMATCH


def test_only_approved_decisions_are_accepted_and_duplicates_are_blocked():
    sim = PaperSimulator()
    decision = approved_decision()
    assert sim.submit(decision, order()).accepted
    duplicate_order = replace(order(), order_id="paper-order-2")
    result = sim.submit(decision, duplicate_order)
    assert not result.accepted
    assert result.reason is SimulationReason.ELIGIBILITY_REJECTED
    assert result.detail == EligibilityReason.DUPLICATE_DECISION.value


def test_cancelled_order_cannot_fill():
    sim = setup_sim()
    cancelled = sim.cancel("paper-order-1")
    assert cancelled.order.status is PaperOrderStatus.CANCELLED
    assert sim.process_snapshot(snapshot("after-cancel")) == ()
    assert not sim.fills


def test_execution_is_disabled_and_no_live_source_exists():
    sim = setup_sim()
    assert sim.EXECUTION_MODE == "DISABLED"
    assert sim.EXECUTION_SOURCE.value == "PAPER_SIMULATOR"
