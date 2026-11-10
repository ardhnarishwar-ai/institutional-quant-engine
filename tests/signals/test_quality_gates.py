import pytest
from core.signals.contracts import RejectionReason
from core.signals.quality_gates import (
    QualityGateInput, QualityGatePolicy, SignalQualityGate,
)


ALLOWED = frozenset({
    "NSE:NSE_SPOT:NIFTY:INR",
    "NSE:NSE_SPOT:BANKNIFTY:INR",
    "NSE:NSE_SPOT:FINNIFTY:INR",
    "BINANCE:CRYPTO_PERPETUAL:BTCUSDT:USDT",
})


def item(**overrides):
    values = dict(venue="NSE", instrument_id="NSE:NSE_SPOT:NIFTY:INR",
                 feed_status="HEALTHY", feed_age_ns=10,
                 book_synchronized=True, history_event_count=200,
                 classified_trade_count=50, exchange_timestamp_ns=100,
                 receive_timestamp_ns=110, source_event_ids=("e1", "e2"),
                 analytics_audit_sequence=2)
    values.update(overrides)
    return QualityGateInput(**values)


def gate(**kwargs):
    return SignalQualityGate(QualityGatePolicy(
        max_feed_age_ns=100, min_history_events=100,
        min_classified_trades=20, allowed_instruments=ALLOWED, **kwargs))


def test_healthy_sufficient_input_is_eligible():
    result = gate().evaluate(item())
    assert result.eligible and result.rejection_reasons == ()


@pytest.mark.parametrize("status,reason", [
    ("STALE", RejectionReason.FEED_STALE),
    ("DESYNCHRONIZED", RejectionReason.FEED_DESYNCHRONIZED),
    ("DISCONNECTED", RejectionReason.FEED_DISCONNECTED),
    ("STARTING", RejectionReason.FEED_NOT_HEALTHY),
])
def test_feed_status_rejects_with_explicit_reason(status, reason):
    result = gate().evaluate(item(feed_status=status))
    assert not result.eligible and reason in result.rejection_reasons


def test_feed_age_threshold_is_inclusive():
    assert gate().evaluate(item(feed_age_ns=100)).eligible
    assert RejectionReason.FEED_STALE in gate().evaluate(item(feed_age_ns=101)).rejection_reasons


def test_desynchronized_book_is_rejected():
    result = gate().evaluate(item(book_synchronized=False))
    assert RejectionReason.BOOK_NOT_SYNCHRONIZED in result.rejection_reasons


@pytest.mark.parametrize("field,value,reason", [
    ("history_event_count", 99, RejectionReason.INSUFFICIENT_HISTORY),
    ("classified_trade_count", 19, RejectionReason.INSUFFICIENT_CLASSIFIED_TRADES),
])
def test_insufficient_history_is_rejected(field, value, reason):
    assert reason in gate().evaluate(item(**{field: value})).rejection_reasons


def test_missing_exchange_timestamp_is_not_substituted():
    result = gate().evaluate(item(exchange_timestamp_ns=None))
    assert RejectionReason.MISSING_EXCHANGE_TIMESTAMP in result.rejection_reasons


def test_missing_provenance_is_rejected():
    result = gate().evaluate(item(source_event_ids=()))
    assert RejectionReason.MISSING_PROVENANCE in result.rejection_reasons


def test_exchange_and_receive_clocks_are_independent_domains():
    result = gate().evaluate(item(exchange_timestamp_ns=9_000_000_000, receive_timestamp_ns=110))
    assert result.eligible


@pytest.mark.parametrize("instrument", sorted(ALLOWED))
def test_instrument_allowlist_keeps_each_market_identity_isolated(instrument):
    result = gate().evaluate(item(instrument_id=instrument))
    assert result.eligible and result.instrument_id == instrument


def test_unknown_instrument_is_rejected():
    result = gate().evaluate(item(instrument_id="NSE:NSE_SPOT:RELIANCE:INR"))
    assert RejectionReason.INSTRUMENT_NOT_ALLOWED in result.rejection_reasons


def test_multiple_failures_are_reported_deterministically():
    bad = item(feed_status="DESYNCHRONIZED", feed_age_ns=1000,
               book_synchronized=False, history_event_count=0,
               classified_trade_count=0, exchange_timestamp_ns=None,
               source_event_ids=())
    first = gate().evaluate(bad)
    for _ in range(20):
        assert gate().evaluate(bad) == first
    assert set(first.rejection_reasons) == {
        RejectionReason.FEED_DESYNCHRONIZED, RejectionReason.FEED_STALE,
        RejectionReason.BOOK_NOT_SYNCHRONIZED, RejectionReason.INSUFFICIENT_HISTORY,
        RejectionReason.INSUFFICIENT_CLASSIFIED_TRADES,
        RejectionReason.MISSING_EXCHANGE_TIMESTAMP, RejectionReason.MISSING_PROVENANCE,
    }


def test_property_gate_never_accepts_any_stale_or_desynchronized_case():
    # Dependency-free property sweep: boundary and randomized-looking deterministic grid.
    for age in range(0, 250, 7):
        for status in ("STALE", "DESYNCHRONIZED"):
            result = gate().evaluate(item(feed_age_ns=age, feed_status=status))
            assert not result.eligible
            assert result.rejection_reasons


def test_property_gate_acceptance_requires_all_minimum_counts():
    for history in range(0, 110, 3):
        for classified in range(0, 25, 2):
            result = gate().evaluate(item(history_event_count=history,
                                          classified_trade_count=classified))
            expected = history >= 100 and classified >= 20
            assert result.eligible is expected
