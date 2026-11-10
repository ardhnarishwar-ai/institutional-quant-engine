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


def test_cross_feed_timestamp_skew_flag():
    agg = MarketStateAggregator(MarketStateConfig(
        max_component_age_ms=1000, max_cross_feed_skew_ms=50))
    agg.update("gex", 1, 100)
    agg.update("vpin", 2, 200)
    state = agg.snapshot(220)
    assert state.cross_feed_skew_ms == 100
    assert "CROSS_FEED_SKEW" in state.flags


def test_sequence_numbers_must_increase_per_component():
    agg = MarketStateAggregator()
    agg.update("gex", 1, 100, sequence_number=10, sequence_domain="nse-feed")
    with pytest.raises(ValueError):
        agg.update("gex", 2, 101, sequence_number=10, sequence_domain="nse-feed")


def test_same_domain_cross_feed_sequence_alignment():
    agg = MarketStateAggregator(MarketStateConfig(
        max_component_age_ms=1000, max_cross_feed_skew_ms=1000,
        max_cross_feed_sequence_skew=2))
    agg.update("gex", 1, 100, sequence_number=101, sequence_domain="shared")
    agg.update("vpin", 2, 101, sequence_number=105, sequence_domain="shared")
    state = agg.snapshot(110)
    assert state.cross_feed_sequence_skew == 4
    assert "CROSS_FEED_SEQUENCE_SKEW" in state.flags


def test_independent_sequence_domains_are_not_compared():
    agg = MarketStateAggregator()
    agg.update("gex", 1, 100, sequence_number=900, sequence_domain="nse")
    agg.update("vpin", 2, 101, sequence_number=4, sequence_domain="crypto")
    state = agg.snapshot(110)
    assert state.cross_feed_sequence_skew is None
    assert "MIXED_SEQUENCE_DOMAINS" in state.flags


def test_rejects_time_regression():
    agg = MarketStateAggregator()
    agg.update("gex", 1, 100)
    with pytest.raises(ValueError):
        agg.update("gex", 2, 99)
