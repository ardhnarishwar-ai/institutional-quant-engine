from decimal import Decimal

from benchmarks.scenarios.catalog import INSTRUMENTS
from core.paper.costs import FeeSchedule, TransactionCostModel
from core.paper.ledger import PaperPortfolioLedger


def test_four_instruments_use_separate_ledgers_and_currency_models():
    ledgers = {}
    for profile in INSTRUMENTS:
        ledger = PaperPortfolioLedger(
            f"benchmark-{profile.key}",
            Decimal("100000"),
            TransactionCostModel(FeeSchedule(), profile.currency),
        )
        ledgers[profile.key] = ledger

    assert ledgers["NIFTY"] is not ledgers["BANKNIFTY"]
    assert ledgers["NIFTY"] is not ledgers["FINNIFTY"]
    assert ledgers["BTCUSDT"] is not ledgers["NIFTY"]
    assert {key: ledger.cost_model.currency for key, ledger in ledgers.items()} == {
        "NIFTY": "INR", "BANKNIFTY": "INR",
        "FINNIFTY": "INR", "BTCUSDT": "USDT",
    }


def test_ledger_positions_are_instrument_keyed_and_not_shared():
    ledger = PaperPortfolioLedger(
        "isolated", Decimal("1000"),
        TransactionCostModel(FeeSchedule(), "INR"),
    )
    nifty = ledger.position("NSE", "NIFTY")
    banknifty = ledger.position("NSE", "BANKNIFTY")
    assert nifty.instrument_id != banknifty.instrument_id
    assert nifty.quantity == Decimal("0")
    assert banknifty.quantity == Decimal("0")
