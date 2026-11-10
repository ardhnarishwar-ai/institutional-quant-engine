from core.microstructure.orderbook_reconstruction import OrderBookConfig, OrderBookReconstruction


def setup_book():
    book = OrderBookReconstruction(OrderBookConfig(
        refill_window_ms=1000, min_depletion=1, confirmation_ratio=.5))
    book.apply_depth(0, "BID", 99, 100)
    book.apply_depth(0, "BID", 98, 100)
    book.apply_depth(0, "ASK", 101, 100)
    book.apply_depth(0, "ASK", 102, 100)
    return book


def test_refill_confirmed_while_best_price_preserved():
    book = setup_book()
    book.apply_depth(100, "BID", 99, 60)   # depletion 40
    book.apply_depth(200, "BID", 99, 85)   # refill 25
    result = book.apply_trade(250, 99, 40, "SELL")
    assert result is not None
    assert result.best_price_preserved
    assert result.confirmed


def test_no_confirmation_after_price_slippage():
    book = setup_book()
    book.apply_depth(100, "BID", 99, 60)
    book.apply_depth(150, "BID", 99, 0)    # best bid falls to 98
    book.apply_depth(200, "BID", 99, 30)   # stale level replenishes
    result = book.apply_trade(250, 99, 40, "SELL")
    assert result is not None
    assert not result.best_price_preserved
    assert not result.confirmed


def test_rejects_out_of_order_depth():
    book = setup_book()
    book.apply_depth(100, "BID", 99, 90)
    try:
        book.apply_depth(50, "BID", 99, 80)
        assert False
    except ValueError:
        pass
