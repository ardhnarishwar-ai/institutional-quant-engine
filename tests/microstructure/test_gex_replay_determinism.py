from datetime import timedelta

from core.microstructure.gex_calculator import GEXCalculator
from tests.microstructure.test_gex_hardening import AS_OF, AS_OF_NS, row


def replay():
    chain = [
        row(95, "C", 1, oi=1200, days=20),
        row(100, "C", -1, oi=2500, days=35),
        row(100, "P", 1, oi=1800, days=35),
        row(105, "P", -1, oi=900, days=50),
    ]
    return GEXCalculator().calculate(
        chain, 100, as_of_timestamp_ns=AS_OF_NS,
        grid_points=501, grid_width=0.15,
    )


def test_repeated_net_gex_replay_is_bitwise_deterministic():
    results = [replay() for _ in range(10)]
    assert all(result == results[0] for result in results)
    assert results[0].as_of_timestamp_ns == AS_OF_NS
    assert results[0].call_gex == results[-1].call_gex
    assert results[0].put_gex == results[-1].put_gex
