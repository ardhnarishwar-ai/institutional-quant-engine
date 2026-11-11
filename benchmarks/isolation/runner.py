"""Stage 5C.4.4 independent cross-instrument isolation benchmark.

Uses only public paper simulator, orchestrator, ledger, analytics and policy
interfaces. No production module is patched or modified.
"""
from __future__ import annotations

import hashlib
import json
from decimal import Decimal as D
from pathlib import Path

from benchmarks.paper_stress.workload import (
    PROFILES, make_stack, make_snapshot, submit_batch, run_profile_workload,
)
from core.paper.ledger import PaperPortfolioLedger
from core.paper.costs import FeeSchedule, TransactionCostModel
from core.microstructure.analytics_state import AnalyticsRegistry
from core.signals.fusion_config import DEFAULT_POLICIES
from core.risk.policy import DEFAULT_RISK_POLICIES
from core.paper.policies import (
    InstrumentSimulationPolicy, InstrumentPolicyRegistry,
)

def _normal(value):
    if isinstance(value, D):
        return format(value, "f")
    if isinstance(value, dict):
        return {str(k): _normal(value[k]) for k in sorted(value, key=str)}
    if isinstance(value, (tuple, list)):
        return [_normal(v) for v in value]
    if hasattr(value, "value"):
        return value.value
    if hasattr(value, "__dataclass_fields__"):
        return {k: _normal(getattr(value, k)) for k in value.__dataclass_fields__}
    return value

def canonical_sha256(value) -> str:
    payload = json.dumps(_normal(value), sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()

def _interleaved_observations(order_count: int = 32):
    stacks = {}
    for profile in PROFILES:
        orchestrator, simulator, ledger = make_stack(profile)
        accepted = submit_batch(profile, simulator, order_count)
        stacks[profile.key] = {
            "profile": profile, "orchestrator": orchestrator,
            "simulator": simulator, "ledger": ledger, "accepted": accepted,
            "first_snapshot_results": 0, "second_snapshot_results": 0,
        }

    # Deterministic round-robin interleaving: every venue/instrument receives
    # snapshot 1 before any receives snapshot 2.
    for snapshot_index, displayed in ((1, D("1")), (2, D("10"))):
        for profile in PROFILES:
            stack = stacks[profile.key]
            result = stack["orchestrator"].process_snapshot(
                make_snapshot(profile, snapshot_index, displayed)
            )
            key = "first_snapshot_results" if snapshot_index == 1 else "second_snapshot_results"
            stack[key] = len(result.simulation_results)

    observations = {}
    for profile in PROFILES:
        stack = stacks[profile.key]
        simulator, ledger = stack["simulator"], stack["ledger"]
        portfolio = ledger.mark_to_market({})
        if len(ledger.entries) != len(simulator.fills):
            raise AssertionError(f"{profile.key}: ledger/fill count mismatch")
        if len({entry.fill_id for entry in ledger.entries}) != len(ledger.entries):
            raise AssertionError(f"{profile.key}: duplicate ledger fill")
        if ledger.cost_model.currency != profile.currency or portfolio.currency != profile.currency:
            raise AssertionError(f"{profile.key}: currency mismatch")
        if any(position.currency != profile.currency for position in ledger.positions):
            raise AssertionError(f"{profile.key}: position currency mismatch")
        if simulator.EXECUTION_MODE != "DISABLED":
            raise AssertionError(f"{profile.key}: execution mode unlocked")
        observations[profile.key] = {
            "instrument": profile.key,
            "venue": profile.venue,
            "instrument_id": profile.instrument_id,
            "currency": profile.currency,
            "accepted_orders": stack["accepted"],
            "fills": len(simulator.fills),
            "ledger_entries": len(ledger.entries),
            "ledger_sequence": portfolio.ledger_sequence,
            "cash": str(portfolio.cash),
            "realized_pnl": str(portfolio.realized_pnl),
            "unrealized_pnl": str(portfolio.unrealized_pnl),
            "execution_mode": simulator.EXECUTION_MODE,
            "first_snapshot_results": stack["first_snapshot_results"],
            "second_snapshot_results": stack["second_snapshot_results"],
        }
    return observations

def validate_registry_contracts():
    expected = {p.instrument_id: p for p in PROFILES}
    tick_sizes = {p.instrument_id: float(p.tick) for p in PROFILES}
    analytics = AnalyticsRegistry(tick_size_by_instrument=tick_sizes)
    states = {}
    for profile in PROFILES:
        states[profile.key] = analytics.state_for(profile.venue, profile.instrument_id)
    if len({id(state) for state in states.values()}) != len(PROFILES):
        raise AssertionError("analytics states are shared across instruments")

    for profile in PROFILES:
        fusion = DEFAULT_POLICIES.get(profile.instrument_id)
        risk = DEFAULT_RISK_POLICIES.get(profile.instrument_id)
        if fusion is None or fusion.instrument_id != profile.instrument_id:
            raise AssertionError(f"{profile.key}: fusion policy identity mismatch")
        if risk is None or risk.instrument_id != profile.instrument_id:
            raise AssertionError(f"{profile.key}: risk policy identity mismatch")

    sim_policies = []
    for profile in PROFILES:
        sim_policies.append(InstrumentSimulationPolicy(
            venue=profile.venue,
            instrument_id=profile.instrument_id,
            policy_version="stage5c4.4-test-v1",
            tick_size=profile.tick,
            quantity_step=D("1"),
            minimum_quantity=D("1"),
            maximum_order_quantity=D("100"),
            maximum_position_quantity=D("200"),
            maximum_order_notional=D("1000000000"),
            maximum_spread_bps=D("100"),
            maximum_participation_rate=D("1"),
            maximum_leverage=D("1"),
            execution_mode="DISABLED",
        ))
    registry = InstrumentPolicyRegistry(sim_policies)
    for profile in PROFILES:
        found = registry.get(profile.venue, profile.instrument_id)
        if found is None or found.instrument_id != profile.instrument_id:
            raise AssertionError(f"{profile.key}: simulation policy lookup mismatch")
        for other in PROFILES:
            if other.key != profile.key and registry.get(other.venue, other.instrument_id) is None:
                # Other instruments at the same venue may legitimately exist;
                # exact identity must still resolve to their own policy.
                raise AssertionError(f"{other.key}: missing independent policy")
    return True

def build_report():
    validate_registry_contracts()
    interleaved = _interleaved_observations()
    rows = []
    for profile in PROFILES:
        standalone = run_profile_workload(profile, order_count=32)
        interleaved_result = interleaved[profile.key]
        standalone_digest = canonical_sha256(standalone)
        interleaved_digest = canonical_sha256(interleaved_result)
        rows.append({
            "instrument": profile.key,
            "venue": profile.venue,
            "instrument_id": profile.instrument_id,
            "currency": profile.currency,
            "standalone_sha256": standalone_digest,
            "interleaved_sha256": interleaved_digest,
            "matched": standalone_digest == interleaved_digest,
            "standalone": standalone,
            "interleaved": interleaved_result,
        })
    report = {
        "stage": "5C.4.4",
        "execution_mode": "DISABLED",
        "isolation_groups": [
            "identity", "analytics_state", "risk_policy_and_assessment",
            "paper_portfolio_currency", "interleaved_replay_determinism",
        ],
        "instruments": [p.key for p in PROFILES],
        "currency_map": {p.key: p.currency for p in PROFILES},
        "interleaved_vs_standalone": "PASS" if all(r["matched"] for r in rows) else "FAIL",
        "comparisons": rows,
    }
    report["report_sha256"] = canonical_sha256(report)
    return report

def write_report(path: str | Path):
    report = build_report()
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_normal(report), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if report["interleaved_vs_standalone"] != "PASS":
        raise AssertionError("interleaved and standalone digest comparison failed")
    return report

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default="reports/stage5c4_4_cross_instrument_isolation.json")
    args = parser.parse_args()
    result = write_report(args.output)
    print("STAGE 5C.4.4 CROSS-INSTRUMENT AUDIT:", result["interleaved_vs_standalone"])
    print("Comparisons:", len(result["comparisons"]), "/ 4 matched")
    print("Report SHA-256:", result["report_sha256"])
    print("JSON:", args.output)
