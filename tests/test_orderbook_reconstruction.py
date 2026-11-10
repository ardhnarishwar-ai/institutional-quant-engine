import pytest
from core.microstructure.orderbook_reconstruction import OrderBookConfig, OrderBookReconstruction


def make_book(**kwargs):
    book = OrderBookReconstruction(OrderBookConfig(
        refill_window_ms=1000, max_event_age_ms=100,
        min_depletion=1, confirmation_ratio=.5, **kwargs))
    book.apply_depth(0, "BID", 99, 100)
    book.apply_depth(0, "BID", 98, 100)
    book.apply_depth(0, "ASK", 101, 100)
    book.apply_depth(0, "ASK", 102, 100)
    return book


def test_trade_and_depth_reduction_reconcile_then_refill_confirms():
    book = make_book()
    book.apply_trade(100, 99, 40, "SELL")
    book.apply_depth(110, "BID", 99, 60)
    book.apply_depth(150, "BID", 99, 85)
    result = book.apply_trade(160, 99, 1, "SELL")
    assert result is not None
    assert result.depleted_qty == pytest.approx(40)
    assert result.replenished_qty == pytest.approx(25)
    assert result.confirmed


def test_unmatched_depth_reduction_is_not_execution_or_absorption():
    book = make_book()
    book.apply_depth(100, "BID", 99, 50)
    assert book.apply_trade(500, 99, 50, "SELL") is None


def test_max_event_age_rejects_stale_trade_depth_pair():
    book = make_book()
    book.apply_depth(100, "BID", 99, 50)
    assert book.apply_trade(250, 99, 50, "SELL") is None


def test_strict_best_price_loss_invalidates_candidate_permanently():
    book = make_book()
    book.apply_trade(100, 99, 40, "SELL")
    book.apply_depth(110, "BID", 99, 60)
    book.apply_depth(120, "BID", 99, 0)  # best bid becomes 98
    book.apply_depth(130, "BID", 99, 40) # old price returns, still invalid
    result = book.apply_trade(140, 99, 1, "SELL")
    assert result is not None
    assert not result.best_price_preserved
    assert not result.confirmed


def test_rejects_out_of_order_events():
    book = make_book()
    book.apply_depth(100, "BID", 99, 90)
    with pytest.raises(ValueError):
        book.apply_trade(99, 99, 1, "SELL")
