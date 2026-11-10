import pytest
from core.microstructure.vpin_calculator import VPINCalculator, VPINConfig


def test_exact_bucket_and_vpin():
    calc = VPINCalculator(VPINConfig(volume_bucket_size=10, rolling_buckets=5))
    result = calc.update(1, 10, side="BUY")
    assert result.completed_buckets == 1
    assert result.latest_completed_bucket.vpin == 1.0
    assert result.bucket_fill_percentage == 0


def test_trade_splits_across_multiple_bucket_boundaries():
    calc = VPINCalculator(VPINConfig(volume_bucket_size=10))
    result = calc.update(1, 25, side="BUY")
    assert result.completed_buckets == 2
    assert result.current_bucket_buy == pytest.approx(5)
    assert result.current_bucket_sell == 0


def test_mixed_side_exact_boundary_fixture():
    calc = VPINCalculator(VPINConfig(volume_bucket_size=10, rolling_buckets=3))
    calc.update(1, 6, side="BUY")
    result = calc.update(2, 4, side="SELL")
    assert result.latest_completed_bucket.buy_volume == pytest.approx(6)
    assert result.latest_completed_bucket.sell_volume == pytest.approx(4)
    assert result.latest_completed_bucket.vpin == pytest.approx(.2)


def test_prior_only_midrank_percentile_and_no_self_inclusion():
    calc = VPINCalculator(VPINConfig(
        volume_bucket_size=10, rolling_buckets=1,
        rank_window=10, min_rank_observations=2))
    calc.update(1, 10, side="BUY")  # rolling VPIN 1.0, reference [1.0]
    calc.update(2, 10, side="BUY")  # prior reference has one item: not enough
    result = calc.update(3, 10, side="BUY")  # prior refs [1.0, 1.0]
    assert result.rolling_cdf_toxicity_rank == pytest.approx(.5)


def test_tick_rule_requires_seed_then_carries_direction():
    calc = VPINCalculator(VPINConfig(volume_bucket_size=100))
    with pytest.raises(ValueError):
        calc.update(1, 1, price=100)
    calc.update(2, 1, side="BUY", price=100)
    result = calc.update(3, 1, price=101)
    assert result.current_bucket_buy == 2


def test_invalid_quantity():
    calc = VPINCalculator(VPINConfig(volume_bucket_size=10))
    with pytest.raises(ValueError):
        calc.update(1, 0, side="BUY")
