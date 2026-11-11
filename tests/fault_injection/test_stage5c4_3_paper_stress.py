from __future__ import annotations

from dataclasses import replace
from decimal import Decimal as D

import pytest

from benchmarks.paper_stress.workload import (
    PROFILES, make_decision, make_order, make_snapshot, make_stack,
    make_synthetic_paper_fill, run_profile_workload, submit_batch,
)
from benchmarks.paper_stress.runner import build_report, canonical_digest
from core.paper.ledger import LedgerRejectReason


@pytest.mark.parametrize("profile", PROFILES, ids=lambda p: p.key)
def test_order_pressure_public_submit_interface(profile):
    orchestrator, simulator, ledger = make_stack(profile)
    accepted = submit_batch(profile, simulator, 32)
    assert accepted == 32
    assert len(simulator.orders) == 32
    assert all(order.status.value == "ACTIVE" for order in simulator.orders)
    assert simulator.EXECUTION_MODE == "DISABLED"
    assert ledger.entries == ()


@pytest.mark.parametrize("profile", PROFILES, ids=lambda p: p.key)
def test_fill_pressure_partial_then_complete_through_orchestrator(profile):
    orchestrator, simulator, ledger = make_stack(profile)
    submit_batch(profile, simulator, 12)
    first = orchestrator.process_snapshot(make_snapshot(profile, 1, D("1")))
    assert len(first.ledger_results) == 12
    assert all(result.accepted for result in first.ledger_results)
    assert all(order.filled_quantity == D("1") for order in simulator.orders)
    with pytest.raises(ValueError, match="duplicate orchestrator snapshot"):
        orchestrator.process_snapshot(make_snapshot(profile, 1, D("1")))
    assert len(ledger.entries) == 12
    second = orchestrator.process_snapshot(make_snapshot(profile, 2, D("10")))
    assert len(second.ledger_results) == 12
    assert all(result.accepted for result in second.ledger_results)
    assert all(order.status.value == "FILLED" for order in simulator.orders)
    assert len(simulator.fills) == len(ledger.entries) == 24


@pytest.mark.parametrize("profile", PROFILES, ids=lambda p: p.key)
def test_ledger_pressure_currency_and_duplicate_accounting_invariants(profile):
    orchestrator, simulator, ledger = make_stack(profile)
    fills = [make_synthetic_paper_fill(profile, i) for i in range(64)]
    accepted = [ledger.apply_fill(fill) for fill in fills]
    assert all(item.accepted for item in accepted)
    assert len(ledger.entries) == 64
    assert len({entry.fill_id for entry in ledger.entries}) == 64
    duplicate = ledger.apply_fill(fills[0])
    assert not duplicate.accepted
    assert duplicate.reason is LedgerRejectReason.DUPLICATE_FILL
    snapshot = ledger.mark_to_market({})
    assert snapshot.currency == profile.currency
    assert all(position.currency == profile.currency for position in ledger.positions)
    assert all(entry.costs.currency == profile.currency for entry in ledger.entries)
    assert ledger.cost_model.currency == profile.currency


@pytest.mark.parametrize("profile", PROFILES, ids=lambda p: p.key)
def test_dual_run_sha256_replay_stability(profile):
    first = run_profile_workload(profile)
    second = run_profile_workload(profile)
    assert canonical_digest(first) == canonical_digest(second)
    assert first["execution_mode"] == second["execution_mode"] == "DISABLED"
    assert first["currency"] == second["currency"] == profile.currency
    if profile.key == "NIFTY":
        report = build_report()
        assert report["instrument_count"] == 4
        assert report["determinism"] == "PASS"
        assert {row["instrument"] for row in report["instruments"]} == {
            "NIFTY", "BANKNIFTY", "FINNIFTY", "BTCUSDT"
        }
        unsigned = {key: value for key, value in report.items() if key != "report_sha256"}
        assert report["report_sha256"] == canonical_digest(unsigned)
    # Feed-health and book-sync vetoes are exercised with an isolated fresh stack.
    orchestrator, simulator, ledger = make_stack(profile)
    submit_batch(profile, simulator, 1)
    stale = replace(make_snapshot(profile, 1, D("10")), feed_healthy=False)
    desync = replace(make_snapshot(profile, 2, D("10")), book_synchronized=False)
    orchestrator.process_snapshot(stale)
    orchestrator.process_snapshot(desync)
    assert simulator.fills == ()
    assert ledger.entries == ()
    assert simulator.EXECUTION_MODE == "DISABLED"
