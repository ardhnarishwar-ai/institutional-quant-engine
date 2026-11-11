from adapters.base.events import EventEnvelope
from benchmarks.orchestrator_executor import fixture_events
from benchmarks.scenarios.catalog import build_scenario_matrix

def test_all_scenario_inputs_are_canonical_and_have_independent_clocks():
    for spec in build_scenario_matrix():
        events=fixture_events(spec)
        assert events and all(isinstance(x,EventEnvelope) for x in events)
        assert all(x.exchange_timestamp_ns is not None for x in events)
        assert all(x.receive_timestamp_ns is not None for x in events)
        assert all(x.exchange_timestamp_ns!=x.receive_timestamp_ns for x in events)
        assert len({x.event_id for x in events})==len(events)
