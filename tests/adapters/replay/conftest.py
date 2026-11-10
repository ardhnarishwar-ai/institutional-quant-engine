"""Reusable synthetic event fixtures for venue-shaped replay scenarios."""
import pytest

from adapters.base.events import EventEnvelope, EventType, TradeEvent
from adapters.base.instrument import InstrumentId, InstrumentKind


@pytest.fixture
def nse_style_trade():
    instrument = InstrumentId("NSE", InstrumentKind.NSE_SPOT, "NIFTY")
    return EventEnvelope("NSE", instrument, EventType.TRADE, 1_000_000,
                         1_000_500, 1, "nse-trades",
                         TradeEvent(24000.0, 25.0, "BUY"), "nse-trade-1")


@pytest.fixture
def crypto_style_trade():
    instrument = InstrumentId("BINANCE", InstrumentKind.CRYPTO_PERPETUAL,
                               "BTCUSDT", quote_currency="USDT")
    return EventEnvelope("BINANCE", instrument, EventType.TRADE, 2_000_000,
                         2_000_250, 1, "binance-trades",
                         TradeEvent(68000.5, 0.002, "SELL"), "crypto-trade-1")
