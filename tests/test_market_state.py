import pytest
from core.models.market_state import MarketStateAggregator, MarketStateConfig


def test_stale_components_are_suppressed():
    agg = MarketStateAggregator(MarketStateConfig(max_component_age_ms=100))
    agg.update("gex", {"net": 1}, 1000)
    state = agg.snapshot(1050)
    assert state.gex == {"net": 1}
    assert "vpin" in state.stale_components
    assert state.vpin is None
    assert "STALE_COMPONENTS" in state.flags


def test_cross_feed_skew_flag():
    agg = MarketStateAggregator(MarketStateConfig(
        max_component_age_ms=1000, max_cross_feed_skew_ms=50))
    agg.update("gex", 1, 100)
    agg.update("vpin", 2, 200)
    state = agg.snapshot(220)
    assert state.cross_feed_skew_ms == 100
    assert "CROSS_FEED_SKEW" in state.flags


def test_rejects_time_regression():
    agg = MarketStateAggregator()
    agg.update("gex", 1, 100)
    with pytest.raises(ValueError):
        agg.update("gex", 2, 99)
