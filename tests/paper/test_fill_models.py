from decimal import Decimal

import pytest

from core.paper.contracts import PaperOrder, PaperOrderType
from core.paper.fill_models import BookLevel, FillReason, MarketSnapshot, TopOfBookFillModel


def order(direction="LONG", limit="101", qty="3"):
    return PaperOrder(
        order_id="o1", decision_id="d1", venue="NSE", instrument_id="NIFTY",
        direction=direction, order_type=PaperOrderType.LIMIT,
        limit_price=Decimal(limit), quantity=Decimal(qty),
        created_exchange_timestamp_ns=None, created_receive_timestamp_ns=10,
        created_simulation_timestamp_ns=11, source_event_ids=("decision-event",),
    )


def snapshot(**kw):
    values = dict(
        snapshot_id="snap-1", venue="NSE", instrument_id="NIFTY",
        bids=(BookLevel(100, 5),), asks=(BookLevel(101, 2),),
        event_ids=("quote-1", "depth-1"), exchange_timestamp_ns=None,
        receive_timestamp_ns=20, simulation_timestamp_ns=21,
    )
    values.update(kw)
    return MarketSnapshot(**values)


def test_buy_fills_at_ask_only_when_within_limit():
    model = TopOfBookFillModel()
    proposal = model.propose(order(), snapshot())
    assert proposal.price == Decimal("101")
    assert proposal.quantity == Decimal("2")
    assert proposal.reason is FillReason.PARTIALLY_FILLED
    assert model.propose(order(limit="100"), snapshot()).reason is FillReason.NOT_MARKETABLE


def test_sell_fills_at_bid_only_when_within_limit():
    proposal = TopOfBookFillModel().propose(order("SHORT", "99"), snapshot())
    assert proposal.price == Decimal("100")
    assert proposal.quantity == Decimal("3")
    assert proposal.reason is FillReason.FILLED


@pytest.mark.parametrize(
    ("changes", "reason"),
    [
        ({"feed_healthy": False}, FillReason.FEED_UNHEALTHY),
        ({"book_synchronized": False}, FillReason.BOOK_DESYNCHRONIZED),
        ({"instrument_id": "BANKNIFTY"}, FillReason.INSTRUMENT_MISMATCH),
        ({"venue": "BINANCE"}, FillReason.VENUE_MISMATCH),
    ],
)
def test_quality_and_isolation_gates(changes, reason):
    assert TopOfBookFillModel().propose(order(), snapshot(**changes)).reason is reason


def test_snapshot_rejects_crossed_book_and_clock_substitution():
    with pytest.raises(ValueError):
        snapshot(bids=(BookLevel(102, 1),), asks=(BookLevel(101, 1),))
    with pytest.raises(ValueError):
        snapshot(exchange_timestamp_ns=-1)
    item = snapshot()
    assert item.exchange_timestamp_ns is None
    assert item.receive_timestamp_ns == 20
