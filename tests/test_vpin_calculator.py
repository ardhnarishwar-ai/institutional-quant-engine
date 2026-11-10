import pytest
from core.microstructure.vpin_calculator import VPINCalculator, VPINConfig


def test_exact_bucket_and_vpin():
    calc = VPINCalculator(VPINConfig(volume_bucket_size=10, rolling_buckets=5))
    result = calc.update(1, 10, side="BUY")
    assert result.completed_buckets == 1
    assert result.latest_completed_bucket.vpin == 1.0
    assert result.bucket_fill_percentage == 0


def test_fractional_trade_splits_across_buckets():
    calc = VPINCalculator(VPINConfig(volume_bucket_size=10))
    result = calc.update(1, 25, side="BUY")
    assert result.completed_buckets == 2
    assert result.current_bucket_buy == pytest.approx(5)
    assert result.current_bucket_sell == 0


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
