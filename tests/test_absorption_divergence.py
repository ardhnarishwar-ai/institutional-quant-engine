from core.microstructure.absorption_divergence import AbsorptionConfig, AbsorptionDetector


def test_buy_pressure_absorbed_near_reference_level():
    detector = AbsorptionDetector(AbsorptionConfig(
        window_ticks=5, min_volume=10, max_abs_price_change_bps=2,
        min_delta_ratio=.6, level_tolerance_bps=5))
    detector.update(1, 100, 10, "BUY", 100)
    result = detector.update(2, 100.001, 10, "BUY", 100)
    assert result.signal == "BUY_ABSORPTION_CANDIDATE"
    assert result.absorbing_side == "SELL"


def test_no_signal_without_reference_level():
    detector = AbsorptionDetector()
    result = detector.update(1, 100, 5, "SELL")
    assert result.signal == "NONE"


def test_invalid_aggressor_rejected():
    detector = AbsorptionDetector()
    try:
        detector.update(1, 100, 1, "UNKNOWN")
        assert False
    except ValueError:
        pass
