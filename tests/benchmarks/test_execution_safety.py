from benchmarks.scenarios.catalog import build_scenario_matrix
from core.paper.simulator import PaperSimulator


def test_paper_simulator_execution_mode_is_hard_disabled():
    assert PaperSimulator.EXECUTION_MODE == "DISABLED"


def test_scenario_contracts_cannot_enable_execution(scenario_matrix):
    assert all(spec.execution_mode == "DISABLED" for spec in scenario_matrix)


def test_matrix_contains_all_four_instruments(scenario_matrix):
    assert {spec.instrument.key for spec in scenario_matrix} == {
        "NIFTY", "BANKNIFTY", "FINNIFTY", "BTCUSDT"
    }
