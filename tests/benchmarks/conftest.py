from decimal import Decimal
import pytest

from benchmarks.scenarios.catalog import build_scenario_matrix


@pytest.fixture
def scenario_matrix():
    return build_scenario_matrix()


@pytest.fixture
def deterministic_executor():
    def execute(spec, run_index):
        # Test harness observation only. Real local benchmark integrations
        # should inject Stage 5C.1 orchestration and genuine journal fixtures.
        return {
            "scenario_id": spec.scenario_id,
            "instrument_id": spec.instrument.instrument_id,
            "venue": spec.instrument.venue,
            "currency": spec.instrument.currency,
            "expected_status": spec.expected_status,
            "execution_mode": "DISABLED",
            "position_quantity": Decimal("0"),
            "ledger_cash_currency": spec.instrument.currency,
            "source_event_ids": (f"fixture:{spec.scenario_id}:event-1",),
            "exchange_timestamp_ns": 1_000_000,
            "receive_timestamp_ns": 1_000_100,
            "simulation_timestamp_ns": 1_000_200,
        }
    return execute
