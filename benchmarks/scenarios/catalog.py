"""Twelve scenario templates expanded across four isolated instrument profiles."""
from __future__ import annotations

from .contracts import InstrumentProfile, ScenarioSpec, ScenarioTemplate

INSTRUMENTS = (
    InstrumentProfile("NIFTY", "NSE", "NIFTY", "INR", "NSE_INDEX"),
    InstrumentProfile("BANKNIFTY", "NSE", "BANKNIFTY", "INR", "NSE_INDEX"),
    InstrumentProfile("FINNIFTY", "NSE", "FINNIFTY", "INR", "NSE_INDEX"),
    InstrumentProfile("BTCUSDT", "BINANCE", "BTCUSDT", "USDT", "CRYPTO_PERPETUAL"),
)

SCENARIO_TEMPLATES = (
    ScenarioTemplate("directional_long", "APPROVED", "Directional long candidate"),
    ScenarioTemplate("directional_short", "APPROVED", "Directional short candidate"),
    ScenarioTemplate("conflicting_evidence", "NEUTRAL", "Conflicting directional evidence"),
    ScenarioTemplate("insufficient_history", "FUSION_REJECTED", "Insufficient history"),
    ScenarioTemplate("stale_feed", "REJECTED", "Stale feed hard veto"),
    ScenarioTemplate("desynchronized_book", "REJECTED", "Desynchronized book hard veto"),
    ScenarioTemplate("missing_liquidity", "HOLD", "Missing liquidity metrics fail closed"),
    ScenarioTemplate("marketable_limit", "PAPER_FILL", "Marketable limit with displayed depth"),
    ScenarioTemplate("partial_depth", "PARTIAL_FILL", "Displayed depth supports partial fill"),
    ScenarioTemplate("non_marketable_limit", "NO_FILL", "Limit lacks executable evidence"),
    ScenarioTemplate("duplicate_replay", "DUPLICATE_REJECTED", "Duplicate event replay"),
    ScenarioTemplate("cross_instrument_attempt", "ISOLATION_PRESERVED", "Cross-instrument contamination attempt"),
)


def build_scenario_matrix() -> tuple[ScenarioSpec, ...]:
    """Return 48 specs: each of 12 templates for each of four instruments.

    Option-specific evidence is deliberately not required by any template;
    GEX may be supplied only when genuine source data exists.
    """
    return tuple(
        ScenarioSpec(
            scenario_id=f"{profile.key.lower()}::{template.key}",
            template_key=template.key,
            instrument=profile,
            expected_status=template.expected_status,
            description=template.description,
            requires_options_evidence=template.requires_options_evidence,
        )
        for profile in INSTRUMENTS
        for template in SCENARIO_TEMPLATES
    )
