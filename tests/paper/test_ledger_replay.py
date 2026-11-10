from dataclasses import dataclass
from decimal import Decimal as D

from core.paper.costs import FeeSchedule, TransactionCostModel
from core.paper.ledger import PaperPortfolioLedger


@dataclass(frozen=True)
class Source:
    value: str = "PAPER_SIMULATOR"


@dataclass(frozen=True)
class Fill:
    fill_id: str
    venue: str = "BINANCE"
    instrument_id: str = "BTCUSDT"
    side: str = "BUY"
    price: D = D("50000")
    quantity: D = D("0.01")
    execution_source: Source = Source()
    market_evidence_event_ids: tuple = ("trade-event", "depth-event")
    fill_model_version: str = "replay-fill-v1"
    exchange_timestamp_ns: int | None = 100
    receive_timestamp_ns: int = 101
    simulation_timestamp_ns: int = 102


def replay():
    model = TransactionCostModel(FeeSchedule(fee_bps=D("4")), "USDT")
    ledger = PaperPortfolioLedger("crypto-paper", D("10000"), model)
    ledger.apply_fill(Fill("x1"))
    ledger.apply_fill(Fill("x2", side="SELL", price=D("50100")))
    return ledger


def test_same_fill_journal_replays_to_identical_ledger():
    first, second = replay(), replay()
    assert first.entries == second.entries
    assert first.positions == second.positions
    assert first.cash == second.cash
    assert first.mark_to_market({("BINANCE", "BTCUSDT"): D("50200")}) == (
        second.mark_to_market({("BINANCE", "BTCUSDT"): D("50200")})
    )


def test_ledger_sequence_is_monotonic_and_append_only_view_is_immutable():
    book = replay()
    assert [entry.sequence for entry in book.entries] == [1, 2]
    entries = book.entries
    assert isinstance(entries, tuple)
    assert len(entries) == 2
