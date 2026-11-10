from datetime import datetime, timedelta, timezone

import pytest

from core.microstructure.gex_calculator import GEXCalculator


AS_OF = datetime(2026, 11, 11, 9, 15, tzinfo=timezone.utc)
AS_OF_NS = int(AS_OF.timestamp() * 1_000_000_000)


def row(strike, kind, sign, *, oi=1000, iv=0.25, days=30, multiplier=1):
    return {
        "strike": strike, "option_type": kind,
        "expiry": AS_OF + timedelta(days=days),
        "open_interest": oi, "iv": iv, "contract_multiplier": multiplier,
        "dealer_position_sign": sign,
    }


def test_as_of_event_timestamp_is_preserved_exactly_and_deterministically():
    chain = [row(100, "C", 1), row(100, "P", -1)]
    calc = GEXCalculator()
    a = calc.calculate(chain, 100, as_of_timestamp_ns=AS_OF_NS)
    b = calc.calculate(iter(chain), 100, as_of_timestamp_ns=AS_OF_NS)
    assert a == b
    assert a.as_of_timestamp_ns == AS_OF_NS
    assert a.as_of_iso.startswith("2026-11-11T09:15:00")


def test_datetime_as_of_matches_nanosecond_clock_for_whole_microseconds():
    chain = [row(100, "C", 1)]
    calc = GEXCalculator()
    a = calc.calculate(chain, 100, as_of=AS_OF)
    b = calc.calculate(chain, 100, as_of_timestamp_ns=AS_OF_NS)
    assert a == b


def test_strike_wise_call_and_put_gex_are_separate_and_aggregated():
    chain = [
        row(100, "C", 1, oi=1000),
        row(105, "C", -1, oi=500),
        row(100, "P", -1, oi=800),
        row(95, "P", 1, oi=300),
    ]
    result = GEXCalculator().calculate(chain, 100, as_of_timestamp_ns=AS_OF_NS)
    assert set(result.call_gex) == {100.0, 105.0}
    assert set(result.put_gex) == {95.0, 100.0}
    assert result.call_gex[100.0] > 0
    assert result.call_gex[105.0] < 0
    assert result.put_gex[100.0] < 0
    assert result.put_gex[95.0] > 0
    assert result.net_gex == pytest.approx(sum(result.call_gex.values()) + sum(result.put_gex.values()))


def test_call_and_put_walls_select_largest_absolute_strike_exposure():
    chain = [
        row(95, "C", 1, oi=100),
        row(100, "C", 1, oi=5000),
        row(105, "P", -1, oi=100),
        row(100, "P", -1, oi=4000),
    ]
    result = GEXCalculator().calculate(chain, 100, as_of_timestamp_ns=AS_OF_NS)
    assert result.call_wall == 100
    assert result.put_wall == 100


def test_opposite_dealer_signs_change_net_gex_sign():
    long = [row(100, "C", 1)]
    short = [row(100, "C", -1)]
    calc = GEXCalculator()
    a = calc.calculate(long, 100, as_of_timestamp_ns=AS_OF_NS)
    b = calc.calculate(short, 100, as_of_timestamp_ns=AS_OF_NS)
    assert a.net_gex > 0
    assert b.net_gex < 0
    assert a.net_gex == pytest.approx(-b.net_gex)


def test_missing_dealer_position_sign_is_rejected_not_inferred_from_oi():
    item = row(100, "C", 1)
    del item["dealer_position_sign"]
    with pytest.raises(ValueError, match="dealer_position_sign"):
        GEXCalculator().calculate([item], 100, as_of_timestamp_ns=AS_OF_NS)


def test_invalid_dealer_sign_and_nonfinite_inputs_are_rejected():
    item = row(100, "C", 0)
    with pytest.raises(ValueError, match="dealer_position_sign"):
        GEXCalculator().calculate([item], 100, as_of_timestamp_ns=AS_OF_NS)
    item = row(100, "C", 1, iv=float("nan"))
    with pytest.raises(ValueError, match="finite"):
        GEXCalculator().calculate([item], 100, as_of_timestamp_ns=AS_OF_NS)


def test_zero_gamma_crossing_is_repeatable_and_nearest_crossing_selected():
    # Two same-strike option rows with opposing dealer signs can create a
    # net curve that changes sign when their gamma decay differs by expiry.
    chain = [
        row(90, "C", 1, oi=5000, days=5, iv=0.20),
        row(110, "P", -1, oi=5000, days=60, iv=0.35),
    ]
    calc = GEXCalculator()
    a = calc.calculate(chain, 100, as_of_timestamp_ns=AS_OF_NS, grid_points=801)
    b = calc.calculate(chain, 100, as_of_timestamp_ns=AS_OF_NS, grid_points=801)
    assert a == b
    assert a.zero_crossings == tuple(sorted(a.zero_crossings))
    if a.zero_flip is not None:
        assert a.zero_flip in a.zero_crossings


def test_expired_options_have_zero_gamma_but_keep_strike_rows():
    chain = [row(100, "C", 1, days=-1)]
    result = GEXCalculator().calculate(chain, 100, as_of_timestamp_ns=AS_OF_NS)
    assert result.call_gex[100] == 0
    assert result.net_gex == 0


def test_empty_chain_returns_timestamped_zero_result():
    result = GEXCalculator().calculate([], 100, as_of_timestamp_ns=AS_OF_NS)
    assert result.net_gex == 0
    assert result.zero_flip is None
    assert result.as_of_timestamp_ns == AS_OF_NS


def test_invalid_as_of_clock_and_grid_are_rejected():
    with pytest.raises(ValueError, match="as_of_timestamp_ns"):
        GEXCalculator().calculate([], 100, as_of_timestamp_ns=-1)
    with pytest.raises(ValueError, match="not both"):
        GEXCalculator().calculate([], 100, as_of=AS_OF, as_of_timestamp_ns=AS_OF_NS)
    with pytest.raises(ValueError, match="grid_points"):
        GEXCalculator().calculate([], 100, as_of_timestamp_ns=AS_OF_NS, grid_points=2)


def test_replay_order_of_chain_rows_does_not_change_result():
    chain = [
        row(95, "C", 1, oi=400), row(100, "C", -1, oi=800),
        row(100, "P", 1, oi=900), row(105, "P", -1, oi=300),
    ]
    calc = GEXCalculator()
    a = calc.calculate(chain, 100, as_of_timestamp_ns=AS_OF_NS)
    b = calc.calculate(reversed(chain), 100, as_of_timestamp_ns=AS_OF_NS)
    assert a == b
