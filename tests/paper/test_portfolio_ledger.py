from dataclasses import dataclass
from decimal import Decimal as D

from core.paper.costs import FeeSchedule, TransactionCostModel
from core.paper.ledger import AccountingMode, LedgerRejectReason, PaperPortfolioLedger


@dataclass(frozen=True)
class Source:
    value: str


@dataclass(frozen=True)
class Fill:
    fill_id: str
    venue: str = "NSE"
    instrument_id: str = "NIFTY"
    side: str = "BUY"
    price: D = D("100")
    quantity: D = D("2")
    execution_source: Source = Source("PAPER_SIMULATOR")
    market_evidence_event_ids: tuple = ("quote-1", "depth-1")
    fill_model_version: str = "model-v1"
    exchange_timestamp_ns: int | None = None
    receive_timestamp_ns: int = 10
    simulation_timestamp_ns: int = 11


def ledger(mode=AccountingMode.CASH, initial=D("10000")):
    costs = TransactionCostModel(FeeSchedule(fee_bps=D("10")), "INR")
    return PaperPortfolioLedger("paper-account-1", initial, costs, mode)


def test_cash_ledger_buy_sell_realized_and_unrealized():
    book = ledger()
    buy = book.apply_fill(Fill("f1", side="BUY", price=D("100"), quantity=D("2")))
    assert buy.accepted
    assert buy.entry.cash_delta == D("-200.20")
    assert buy.entry.position_after.quantity == D("2")
    assert buy.entry.position_after.average_entry_price == D("100")

    sell = book.apply_fill(Fill("f2", side="SELL", price=D("110"), quantity=D("1")))
    assert sell.accepted
    assert sell.entry.realized_pnl == D("10")
    assert sell.entry.position_after.quantity == D("1")
    assert sell.entry.position_after.average_entry_price == D("100")

    snapshot = book.mark_to_market({("NSE", "NIFTY"): D("120")})
    assert snapshot.unrealized_pnl == D("20")
    assert snapshot.realized_pnl == D("10")
    assert snapshot.market_value == D("120")
    assert snapshot.equity == book.cash + D("120")


def test_duplicate_fill_is_idempotently_rejected():
    book = ledger()
    assert book.apply_fill(Fill("same")).accepted
    result = book.apply_fill(Fill("same"))
    assert not result.accepted
    assert result.reason is LedgerRejectReason.DUPLICATE_FILL
    assert len(book.entries) == 1


def test_only_paper_simulator_fills_are_accepted():
    book = ledger()
    result = book.apply_fill(Fill("live", execution_source=Source("BROKER")))
    assert not result.accepted
    assert result.reason is LedgerRejectReason.NON_PAPER_FILL
    assert book.cash == D("10000")


def test_short_position_and_reversal_accounting():
    book = ledger()
    short = book.apply_fill(Fill("s1", side="SELL", price=D("100"), quantity=D("3")))
    assert short.entry.position_after.quantity == D("-3")
    cover = book.apply_fill(Fill("s2", side="BUY", price=D("90"), quantity=D("4")))
    assert cover.entry.realized_pnl == D("30")
    assert cover.entry.position_after.quantity == D("1")
    assert cover.entry.position_after.average_entry_price == D("90")


def test_linear_derivative_cash_uses_realized_pnl_not_full_notional():
    book = ledger(AccountingMode.LINEAR_DERIVATIVE)
    opened = book.apply_fill(Fill("d1", side="BUY", price=D("100"), quantity=D("2")))
    assert opened.entry.cash_delta == D("-0.20")
    closed = book.apply_fill(Fill("d2", side="SELL", price=D("110"), quantity=D("2")))
    assert closed.entry.realized_pnl == D("20")
    assert closed.entry.cash_delta == D("19.78")
    assert book.position("NSE", "NIFTY").quantity == D("0")


def test_instruments_and_venues_are_isolated():
    book = ledger()
    book.apply_fill(Fill("n1", venue="NSE", instrument_id="NIFTY"))
    book.apply_fill(Fill("b1", venue="BINANCE", instrument_id="BTCUSDT"))
    assert book.position("NSE", "NIFTY").quantity == D("2")
    assert book.position("BINANCE", "BTCUSDT").quantity == D("2")
    assert len(book.positions) == 2


def test_clocks_and_market_provenance_are_retained_exactly():
    book = ledger()
    result = book.apply_fill(Fill(
        "clocks", exchange_timestamp_ns=None,
        receive_timestamp_ns=123456, simulation_timestamp_ns=987654,
    ))
    entry = result.entry
    assert entry.exchange_timestamp_ns is None
    assert entry.receive_timestamp_ns == 123456
    assert entry.simulation_timestamp_ns == 987654
    assert entry.market_evidence_event_ids == ("quote-1", "depth-1")
