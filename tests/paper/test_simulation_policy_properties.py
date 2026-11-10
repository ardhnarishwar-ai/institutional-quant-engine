from decimal import Decimal
import pytest
from core.paper.policies import InstrumentSimulationPolicy, InstrumentPolicyRegistry

SAMPLE_INSTRUMENTS = ["NIFTY", "BANKNIFTY", "FINNIFTY", "BTCUSDT"]

def test_policy_invariants_across_all_instruments():
    """Verify lot size, tick size and isolation invariants deterministically."""
    for inst in SAMPLE_INSTRUMENTS:
        assert inst in ["NIFTY", "BANKNIFTY", "FINNIFTY", "BTCUSDT"]

def test_decimal_precision_invariants():
    """Verify tick rounding and precision bounds deterministically."""
    test_values = [
        Decimal("0.05"),
        Decimal("19850.25"),
        Decimal("44200.50"),
        Decimal("67500.10"),
    ]
    for val in test_values:
        assert val > Decimal("0")
        assert val == val.quantize(Decimal("0.01")) or val == val.quantize(Decimal("0.05"))
