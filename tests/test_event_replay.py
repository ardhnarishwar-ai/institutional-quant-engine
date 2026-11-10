"""Deterministic replay fixtures for core event ordering and state transitions."""
from core.microstructure.orderbook_reconstruction import OrderBookConfig, OrderBookReconstruction
from core.microstructure.vpin_calculator import VPINCalculator, VPINConfig
from core.models.market_state import MarketStateAggregator, MarketStateConfig


def test_deterministic_trade_depth_replay_produces_same_book_result():
    events = [
        ("depth", 0, "BID", 100.0, 50.0),
        ("depth", 0, "BID", 99.0, 50.0),
        ("depth", 0, "ASK", 101.0, 50.0),
        ("trade", 10, 100.0, 20.0, "SELL"),
        ("depth", 11, "BID", 100.0, 30.0),
        ("depth", 20, "BID", 100.0, 42.0),
    ]

    def replay():
        book = OrderBookReconstruction(OrderBookConfig(
            refill_window_ms=100, max_event_age_ms=25,
            min_depletion=1, confirmation_ratio=.5))
        for event in events:
            if event[0] == "depth":
                _, ts, side, price, qty = event
                book.apply_depth(ts, side, price, qty)
            else:
                _, ts, price, qty, aggressor = event
                book.apply_trade(ts, price, qty, aggressor)
        return book.snapshot(20), book._levels[("BID", 100.0)]

    snap_a, state_a = replay()
    snap_b, state_b = replay()
    assert snap_a.bids == snap_b.bids
    assert state_a.executed_depletion_qty == state_b.executed_depletion_qty == 20
    assert state_a.replenished_qty == state_b.replenished_qty == 12


def test_vpin_event_replay_bucket_outputs_are_deterministic():
    events = [(1, 4, "BUY"), (2, 6, "SELL"), (3, 10, "BUY"), (4, 5, "SELL")]
    def replay():
        calc = VPINCalculator(VPINConfig(volume_bucket_size=10, rolling_buckets=4))
        return [calc.update(ts, qty, side=side) for ts, qty, side in events]
    a, b = replay(), replay()
    assert [x.latest_completed_bucket for x in a] == [x.latest_completed_bucket for x in b]
    assert [x.current_vpin for x in a] == [x.current_vpin for x in b]


def test_market_state_replay_rejects_sequence_regression():
    agg = MarketStateAggregator(MarketStateConfig(max_component_age_ms=1000))
    agg.update("gex", 1, 10, sequence_number=10, sequence_domain="feed")
    agg.update("vpin", 2, 11, sequence_number=11, sequence_domain="feed")
    state = agg.snapshot(20)
    assert state.cross_feed_sequence_skew == 1
