from datetime import datetime, timedelta, timezone
import pytest
from core.microstructure.gex_calculator import GEXCalculator


def option(kind="C", sign=1):
    return dict(strike=100, option_type=kind,
                expiry=datetime.now(timezone.utc) + timedelta(days=30),
                open_interest=100, iv=0.25, contract_multiplier=1,
                dealer_position_sign=sign)


def test_gamma_positive_and_gex_signed():
    calc = GEXCalculator()
    assert calc.bs_gamma(100, 100, 30/365, .25) > 0
    long = calc.calculate([option(sign=1)], 100)
    short = calc.calculate([option(sign=-1)], 100)
    assert long.net_gex > 0
    assert short.net_gex < 0


def test_requires_explicit_dealer_sign():
    row = option()
    del row["dealer_position_sign"]
    with pytest.raises(ValueError):
        GEXCalculator().calculate([row], 100)


def test_empty_chain():
    result = GEXCalculator().calculate([], 100)
    assert result.net_gex == 0
    assert result.zero_flip is None
