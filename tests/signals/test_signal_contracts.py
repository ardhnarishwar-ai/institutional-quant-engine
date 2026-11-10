import pytest
from core.signals.contracts import (
    ConfidenceMetrics, ExecutionMode, SignalContractError, SignalDirection,
    SignalEvent, SignalProvenance,
)


def provenance():
    return SignalProvenance(("evt-1", "evt-2"), 100, 110, 2)


def make_signal(**kwargs):
    data = dict(signal_id="sig-1", venue="nse",
                instrument_id="NSE:NSE_SPOT:NIFTY:INR",
                direction=SignalDirection.LONG,
                confidence=ConfidenceMetrics(.72, .8, 40),
                provenance=provenance(), created_at_receive_timestamp_ns=120)
    data.update(kwargs)
    return SignalEvent(**data)


def test_signal_contract_is_immutable_and_execution_disabled():
    signal = make_signal()
    assert signal.venue == "NSE"
    assert signal.execution_mode is ExecutionMode.DISABLED
    with pytest.raises((AttributeError, TypeError)):
        signal.direction = SignalDirection.SHORT


@pytest.mark.parametrize("direction", list(SignalDirection))
def test_all_signal_directions_are_supported(direction):
    assert make_signal(direction=direction).direction is direction


@pytest.mark.parametrize("score", [-.01, 1.01, float("nan"), float("inf"), True])
def test_confidence_score_rejects_invalid_values(score):
    with pytest.raises(SignalContractError):
        ConfidenceMetrics(score, .5, 10)


@pytest.mark.parametrize("coverage", [-1, 1.001, float("nan")])
def test_evidence_coverage_is_bounded(coverage):
    with pytest.raises(SignalContractError):
        ConfidenceMetrics(.5, coverage, 10)


@pytest.mark.parametrize("sample_size", [-1, True, 1.5])
def test_sample_size_must_be_nonnegative_integer(sample_size):
    with pytest.raises(SignalContractError):
        ConfidenceMetrics(.5, .5, sample_size)


def test_provenance_rejects_duplicate_or_empty_ids():
    with pytest.raises(SignalContractError):
        SignalProvenance(("same", "same"), 1, 2, 1)
    with pytest.raises(SignalContractError):
        SignalProvenance((), 1, 2, 1)


def test_missing_exchange_clock_is_preserved():
    p = SignalProvenance(("evt",), None, 100, 1)
    assert p.exchange_timestamp_ns is None


def test_signal_cannot_be_created_before_source_receive_time():
    with pytest.raises(SignalContractError):
        make_signal(created_at_receive_timestamp_ns=109)


def test_execution_cannot_be_enabled():
    with pytest.raises(SignalContractError):
        make_signal(execution_mode="LIVE")


def test_instrument_identity_is_not_rewritten():
    for instrument in ("NSE:NSE_SPOT:NIFTY:INR", "NSE:NSE_SPOT:BANKNIFTY:INR",
                       "NSE:NSE_SPOT:FINNIFTY:INR", "BINANCE:CRYPTO_PERPETUAL:BTCUSDT:USDT"):
        assert make_signal(instrument_id=instrument).instrument_id == instrument
