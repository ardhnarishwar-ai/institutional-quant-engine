from __future__ import annotations

from dataclasses import FrozenInstanceError
from decimal import Decimal as D

import pytest

from benchmarks.paper_stress.workload import (
    PROFILES, make_synthetic_paper_fill,
)
from benchmarks.isolation.runner import (
    build_report, canonical_sha256, validate_registry_contracts,
)
from core.microstructure.analytics_state import AnalyticsRegistry
from core.signals.fusion_config import DEFAULT_POLICIES
from core.risk.policy import DEFAULT_RISK_POLICIES
from core.risk.contracts import RiskAssessment, RiskSeverity
from core.paper.costs import FeeSchedule, TransactionCostModel
from core.paper.ledger import PaperPortfolioLedger
from core.paper.policies import (
    InstrumentSimulationPolicy, InstrumentPolicyRegistry,
)

def _policy(profile):
    return InstrumentSimulationPolicy(
        venue=profile.venue,
        instrument_id=profile.instrument_id,
        policy_version="isolation-test-v1",
        tick_size=profile.tick,
        quantity_step=D("1"),
        minimum_quantity=D("1"),
        maximum_order_quantity=D("100"),
        maximum_position_quantity=D("200"),
        maximum_order_notional=D("1000000000"),
        maximum_spread_bps=D("100"),
        maximum_participation_rate=D("1"),
        execution_mode="DISABLED",
    )

@pytest.mark.parametrize("profile", PROFILES, ids=lambda p: p.key)
def test_identity_and_analytics_state_are_instrument_isolated(profile):
    registry = AnalyticsRegistry(
        tick_size_by_instrument={p.instrument_id: float(p.tick) for p in PROFILES}
    )
    states = {
        p.key: registry.state_for(p.venue, p.instrument_id)
        for p in PROFILES
    }
    own = states[profile.key]
    other_states = [s for key, s in states.items() if key != profile.key]
    assert all(own is not other for other in other_states)
    before = {key: state.snapshot().event_count for key, state in states.items()}
    own.record_unclassified_trade(
        event_id=f"isolation-{profile.key}",
        price=float(profile.price),
        quantity=1.0,
        exchange_timestamp_ns=100,
        receive_timestamp_ns=200,
    )
    after = {key: state.snapshot().event_count for key, state in states.items()}
    assert after[profile.key] == before[profile.key] + 1
    assert all(after[key] == before[key] for key in before if key != profile.key)

@pytest.mark.parametrize("profile", PROFILES, ids=lambda p: p.key)
def test_fusion_and_risk_policies_resolve_only_to_exact_instrument(profile):
    fusion = DEFAULT_POLICIES[profile.instrument_id]
    risk = DEFAULT_RISK_POLICIES[profile.instrument_id]
    assert fusion.instrument_id == profile.instrument_id
    assert risk.instrument_id == profile.instrument_id
    assessment = RiskAssessment(
        instrument_id=profile.instrument_id,
        risk_score=0.1,
        risk_band=RiskSeverity.LOW,
        checks=(),
        hard_veto=False,
        reasons=(),
        policy_version=risk.policy_version,
        audit_sequence=1,
    )
    assert assessment.instrument_id == profile.instrument_id
    assert assessment.policy_version == risk.policy_version
    assert fusion is not DEFAULT_POLICIES.get("UNKNOWN:INSTRUMENT")
    assert risk is not DEFAULT_RISK_POLICIES.get("UNKNOWN:INSTRUMENT")
    with pytest.raises(FrozenInstanceError):
        risk.instrument_id = "CROSS_INSTRUMENT"

@pytest.mark.parametrize("profile", PROFILES, ids=lambda p: p.key)
def test_simulation_policy_registry_exact_identity_and_execution_lock(profile):
    registry = InstrumentPolicyRegistry([_policy(p) for p in PROFILES])
    found = registry.get(profile.venue, profile.instrument_id)
    assert found is not None
    assert found.instrument_id == profile.instrument_id
    assert found.venue == profile.venue
    assert found.execution_mode == "DISABLED"
    assert registry.get("WRONG_VENUE", profile.instrument_id) is None
    assert registry.get(profile.venue, "WRONG_INSTRUMENT") is None
    assert len(registry.keys()) == 4

@pytest.mark.parametrize("profile", PROFILES, ids=lambda p: p.key)
def test_one_currency_ledger_and_instrument_position_isolation(profile):
    currency = profile.currency
    ledger = PaperPortfolioLedger(
        f"isolated-{profile.key}",
        D("1000000000"),
        TransactionCostModel(FeeSchedule(), currency),
    )
    fill = make_synthetic_paper_fill(profile, 0)
    result = ledger.apply_fill(fill)
    assert result.accepted
    assert ledger.cost_model.currency == currency
    assert result.entry.costs.currency == currency
    assert ledger.position(profile.venue, profile.instrument_id).quantity == fill.quantity
    assert all(position.currency == currency for position in ledger.positions)
    assert ledger.mark_to_market({}).currency == currency
    assert len(ledger.entries) == 1

def test_inr_ledger_keeps_three_nse_positions_separately_keyed():
    profiles = [p for p in PROFILES if p.currency == "INR"]
    ledger = PaperPortfolioLedger(
        "shared-INR-account", D("1000000"),
        TransactionCostModel(FeeSchedule(), "INR"),
    )
    for index, profile in enumerate(profiles):
        assert ledger.apply_fill(make_synthetic_paper_fill(profile, index)).accepted
    assert len(ledger.positions) == 3
    assert {position.instrument_id for position in ledger.positions} == {
        profile.instrument_id for profile in profiles
    }
    assert all(position.currency == "INR" for position in ledger.positions)

def test_separate_inr_and_usdt_ledgers_never_share_accounting_state():
    inr_profiles = [p for p in PROFILES if p.currency == "INR"]
    crypto = next(p for p in PROFILES if p.currency == "USDT")
    inr = PaperPortfolioLedger("portfolio-INR", D("1000000"),
        TransactionCostModel(FeeSchedule(), "INR"))
    usdt = PaperPortfolioLedger("portfolio-USDT", D("1000000"),
        TransactionCostModel(FeeSchedule(), "USDT"))
    for index, profile in enumerate(inr_profiles):
        assert inr.apply_fill(make_synthetic_paper_fill(profile, index)).accepted
    assert usdt.apply_fill(make_synthetic_paper_fill(crypto, 0)).accepted
    assert inr.cost_model.currency == "INR"
    assert usdt.cost_model.currency == "USDT"
    assert all(p.currency == "INR" for p in inr.positions)
    assert all(p.currency == "USDT" for p in usdt.positions)
    assert inr.entries[0].account_id != usdt.entries[0].account_id
    assert inr.initial_cash == usdt.initial_cash

def test_independent_registry_contracts_cover_all_four_instruments():
    assert validate_registry_contracts() is True

def test_interleaved_vs_standalone_sha256_matches_all_instruments():
    report = build_report()
    assert report["interleaved_vs_standalone"] == "PASS"
    assert len(report["comparisons"]) == 4
    assert {row["instrument"] for row in report["comparisons"]} == {
        "NIFTY", "BANKNIFTY", "FINNIFTY", "BTCUSDT"
    }
    assert all(row["matched"] for row in report["comparisons"])
    unsigned = {k: v for k, v in report.items() if k != "report_sha256"}
    assert report["report_sha256"] == canonical_sha256(unsigned)
    assert report["execution_mode"] == "DISABLED"
