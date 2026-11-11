from collections import Counter

from benchmarks.scenarios.catalog import INSTRUMENTS, SCENARIO_TEMPLATES, build_scenario_matrix


def test_twelve_templates_expand_across_four_instruments():
    matrix = build_scenario_matrix()
    assert len(SCENARIO_TEMPLATES) == 12
    assert len(INSTRUMENTS) == 4
    assert len(matrix) == 48
    counts = Counter(spec.instrument.key for spec in matrix)
    assert counts == {"NIFTY": 12, "BANKNIFTY": 12, "FINNIFTY": 12, "BTCUSDT": 12}


def test_currency_profiles_are_isolated():
    currencies = {profile.key: profile.currency for profile in INSTRUMENTS}
    assert currencies == {
        "NIFTY": "INR", "BANKNIFTY": "INR",
        "FINNIFTY": "INR", "BTCUSDT": "USDT",
    }


def test_all_scenarios_keep_execution_disabled():
    assert all(spec.execution_mode == "DISABLED" for spec in build_scenario_matrix())


def test_btc_scenarios_do_not_require_fabricated_options_evidence():
    btc_specs = [x for x in build_scenario_matrix() if x.instrument.key == "BTCUSDT"]
    assert btc_specs
    assert not any(x.requires_options_evidence for x in btc_specs)
