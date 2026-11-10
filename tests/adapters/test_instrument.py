import pytest
from adapters.base.instrument import InstrumentId, InstrumentKind


def test_nse_spot_identifier():
    item = InstrumentId("nse", InstrumentKind.NSE_SPOT, "nifty")
    assert item.canonical_id == "NSE:NSE_SPOT:NIFTY:INR"


def test_nse_future_identifier():
    item = InstrumentId("NSE", InstrumentKind.NSE_FUTURE, "BANKNIFTY",
                        expiry="2026-11-26")
    assert item.canonical_id.endswith(":2026-11-26")


def test_nse_option_identifier():
    item = InstrumentId("NSE", InstrumentKind.NSE_OPTION, "NIFTY",
                        expiry="2026-11-26", strike=25000, option_type="CE")
    assert item.canonical_id.endswith(":2026-11-26:25000:CE")


def test_crypto_perpetual_identifier():
    item = InstrumentId("BINANCE", InstrumentKind.CRYPTO_PERPETUAL,
                        "BTCUSDT", quote_currency="USDT")
    assert "BTCUSDT:USDT" in item.canonical_id


def test_rejects_invalid_derivative_fields():
    with pytest.raises(ValueError):
        InstrumentId("NSE", InstrumentKind.NSE_OPTION, "NIFTY",
                     expiry="2026-11-26", strike=0, option_type="CE")
    with pytest.raises(ValueError):
        InstrumentId("NSE", InstrumentKind.NSE_FUTURE, "NIFTY",
                     expiry="26-11-2026")
