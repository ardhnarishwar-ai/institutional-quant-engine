from decimal import Decimal

from core.paper.contracts import PaperOrder, PaperOrderType
from core.paper.fill_models import BookLevel, MarketSnapshot
from core.paper.simulator import PaperSimulator

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




def build_run():
    sim = PaperSimulator()
    order = PaperOrder(
        order_id="replay-order", decision_id="d-1", venue="NSE",
        instrument_id="NSE:NSE_SPOT:NIFTY:INR", direction="LONG",
        order_type=PaperOrderType.LIMIT, limit_price=Decimal("101"),
        quantity=Decimal("3"), created_exchange_timestamp_ns=None,
        created_receive_timestamp_ns=10, created_simulation_timestamp_ns=11,
        source_event_ids=("decision-journal-event",),
    )
    assert sim.submit(approved_decision(), order).accepted
    snapshots = (
        MarketSnapshot(
            snapshot_id="r1", venue="NSE", instrument_id=order.instrument_id,
            bids=(BookLevel(100, 5),), asks=(BookLevel(101, 1),),
            event_ids=("r1-quote", "r1-depth"), exchange_timestamp_ns=None,
            receive_timestamp_ns=20, simulation_timestamp_ns=21,
        ),
        MarketSnapshot(
            snapshot_id="r2", venue="NSE", instrument_id=order.instrument_id,
            bids=(BookLevel(100, 5),), asks=(BookLevel(101, 5),),
            event_ids=("r2-quote", "r2-depth"), exchange_timestamp_ns=22,
            receive_timestamp_ns=23, simulation_timestamp_ns=24,
        ),
    )
    results = sim.replay(snapshots)
    return sim, results


def test_repeated_replay_has_identical_fills_and_final_order_state():
    sim1, results1 = build_run()
    sim2, results2 = build_run()
    assert results1 == results2
    assert sim1.fills == sim2.fills
    assert sim1.orders == sim2.orders
    assert sim1.orders[0].filled_quantity == Decimal("3")
    assert len(sim1.fills) == 2


def test_fill_identifiers_are_stable_across_runs():
    sim1, _ = build_run()
    sim2, _ = build_run()
    assert tuple(x.fill_id for x in sim1.fills) == tuple(x.fill_id for x in sim2.fills)
