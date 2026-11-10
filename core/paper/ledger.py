"""Append-only paper portfolio accounting with fill idempotency."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import Dict, Optional, Tuple

from .costs import CostBreakdown, TransactionCostModel


D = Decimal


class AccountingMode(str, Enum):
    CASH = "CASH"
    LINEAR_DERIVATIVE = "LINEAR_DERIVATIVE"


class LedgerRejectReason(str, Enum):
    DUPLICATE_FILL = "DUPLICATE_FILL"
    NON_PAPER_FILL = "NON_PAPER_FILL"
    INVALID_FILL = "INVALID_FILL"
    CURRENCY_MISMATCH = "CURRENCY_MISMATCH"


@dataclass(frozen=True, slots=True)
class Position:
    account_id: str
    venue: str
    instrument_id: str
    quantity: Decimal  # positive long, negative short
    average_entry_price: Decimal
    realized_pnl: Decimal
    fees_paid: Decimal
    currency: str


@dataclass(frozen=True, slots=True)
class LedgerEntry:
    entry_id: str
    sequence: int
    account_id: str
    fill_id: str
    venue: str
    instrument_id: str
    side: str
    price: Decimal
    quantity: Decimal
    signed_quantity: Decimal
    notional: Decimal
    costs: CostBreakdown
    realized_pnl: Decimal
    cash_delta: Decimal
    cash_after: Decimal
    position_after: Position
    execution_source: str
    market_evidence_event_ids: Tuple[str, ...]
    fill_model_version: str
    exchange_timestamp_ns: Optional[int]
    receive_timestamp_ns: int
    simulation_timestamp_ns: int
    accounting_mode: AccountingMode


@dataclass(frozen=True, slots=True)
class ApplyFillResult:
    accepted: bool
    entry: Optional[LedgerEntry]
    reason: Optional[LedgerRejectReason] = None


class PaperPortfolioLedger:
    """One-currency ledger. FX conversion and cross-currency netting are excluded."""

    def __init__(
        self,
        account_id: str,
        initial_cash: Decimal,
        cost_model: TransactionCostModel,
        accounting_mode: AccountingMode = AccountingMode.CASH,
    ) -> None:
        if not isinstance(account_id, str) or not account_id.strip():
            raise ValueError("account_id must be non-empty")
        initial_cash = self._amount(initial_cash, "initial_cash", allow_zero=True)
        self.account_id = account_id
        self.initial_cash = initial_cash
        self.cash = initial_cash
        self.cost_model = cost_model
        self.accounting_mode = AccountingMode(accounting_mode)
        self._positions: Dict[Tuple[str, str], Position] = {}
        self._entries: list[LedgerEntry] = []
        self._fill_ids: set[str] = set()

    @staticmethod
    def _amount(value: object, name: str, *, allow_zero: bool = False) -> Decimal:
        if isinstance(value, bool):
            raise ValueError(f"{name} must be a finite decimal")
        try:
            result = value if isinstance(value, Decimal) else D(str(value))
        except Exception as exc:
            raise ValueError(f"{name} must be a finite decimal") from exc
        if not result.is_finite() or result < 0 or (not allow_zero and result == 0):
            raise ValueError(f"{name} must be {'non-negative' if allow_zero else 'positive'}")
        return result

    @property
    def entries(self) -> Tuple[LedgerEntry, ...]:
        return tuple(self._entries)

    @property
    def positions(self) -> Tuple[Position, ...]:
        return tuple(self._positions[key] for key in sorted(self._positions))

    def position(self, venue: str, instrument_id: str) -> Position:
        return self._positions.get(
            (venue, instrument_id),
            Position(self.account_id, venue, instrument_id, D("0"), D("0"),
                     D("0"), D("0"), self.cost_model.currency),
        )

    def apply_fill(self, fill: object) -> ApplyFillResult:
        fill_id = getattr(fill, "fill_id", None)
        if not isinstance(fill_id, str) or not fill_id:
            return ApplyFillResult(False, None, LedgerRejectReason.INVALID_FILL)
        if fill_id in self._fill_ids:
            return ApplyFillResult(False, None, LedgerRejectReason.DUPLICATE_FILL)

        source = getattr(getattr(fill, "execution_source", None), "value",
                         getattr(fill, "execution_source", None))
        if source != "PAPER_SIMULATOR":
            return ApplyFillResult(False, None, LedgerRejectReason.NON_PAPER_FILL)

        try:
            venue = fill.venue
            instrument_id = fill.instrument_id
            side = str(fill.side).upper()
            price = self._amount(fill.price, "price")
            quantity = self._amount(fill.quantity, "quantity")
            evidence = tuple(fill.market_evidence_event_ids)
            version = fill.fill_model_version
            exchange_ns = fill.exchange_timestamp_ns
            receive_ns = fill.receive_timestamp_ns
            simulation_ns = fill.simulation_timestamp_ns
            if side not in ("BUY", "SELL") or not venue or not instrument_id:
                raise ValueError("invalid fill identity or side")
            if not evidence or any(not isinstance(x, str) or not x for x in evidence):
                raise ValueError("market evidence IDs are required")
            if not isinstance(version, str) or not version:
                raise ValueError("fill model version is required")
            if exchange_ns is not None and (
                isinstance(exchange_ns, bool) or not isinstance(exchange_ns, int) or exchange_ns < 0
            ):
                raise ValueError("invalid exchange timestamp")
            for value in (receive_ns, simulation_ns):
                if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                    raise ValueError("invalid receive/simulation timestamp")
        except Exception:
            return ApplyFillResult(False, None, LedgerRejectReason.INVALID_FILL)

        costs = self.cost_model.calculate(price, quantity)
        key = (venue, instrument_id)
        old = self.position(venue, instrument_id)
        signed = quantity if side == "BUY" else -quantity
        old_qty = old.quantity
        new_qty = old_qty + signed
        realized = D("0")

        if old_qty == 0 or (old_qty > 0) == (signed > 0):
            new_avg = (
                (abs(old_qty) * old.average_entry_price + quantity * price)
                / (abs(old_qty) + quantity)
            )
        else:
            closing_qty = min(abs(old_qty), quantity)
            realized = closing_qty * (price - old.average_entry_price) * (
                D("1") if old_qty > 0 else D("-1")
            )
            if new_qty == 0:
                new_avg = D("0")
            elif (new_qty > 0) != (old_qty > 0):
                new_avg = price
            else:
                new_avg = old.average_entry_price

        if self.accounting_mode is AccountingMode.CASH:
            principal_delta = -price * quantity if side == "BUY" else price * quantity
            cash_delta = principal_delta - costs.total
        else:
            cash_delta = realized - costs.total

        cash_after = self.cash + cash_delta
        new_position = Position(
            account_id=self.account_id, venue=venue, instrument_id=instrument_id,
            quantity=new_qty, average_entry_price=new_avg,
            realized_pnl=old.realized_pnl + realized,
            fees_paid=old.fees_paid + costs.total, currency=self.cost_model.currency,
        )
        seq = len(self._entries) + 1
        material = f"{self.account_id}|{seq}|{fill_id}|{cash_after}"
        entry_id = "ledger-" + hashlib.sha256(material.encode()).hexdigest()[:24]
        entry = LedgerEntry(
            entry_id=entry_id, sequence=seq, account_id=self.account_id,
            fill_id=fill_id, venue=venue, instrument_id=instrument_id, side=side,
            price=price, quantity=quantity, signed_quantity=signed,
            notional=price * quantity, costs=costs, realized_pnl=realized,
            cash_delta=cash_delta, cash_after=cash_after, position_after=new_position,
            execution_source=source, market_evidence_event_ids=evidence,
            fill_model_version=version, exchange_timestamp_ns=exchange_ns,
            receive_timestamp_ns=receive_ns, simulation_timestamp_ns=simulation_ns,
            accounting_mode=self.accounting_mode,
        )

        # State mutation occurs only after all validation and calculations succeed.
        self._entries.append(entry)
        self._fill_ids.add(fill_id)
        self._positions[key] = new_position
        self.cash = cash_after
        return ApplyFillResult(True, entry)

    def mark_to_market(self, marks: Dict[Tuple[str, str], Decimal]) -> "PortfolioSnapshot":
        unrealized = D("0")
        market_value = D("0")
        marked = []
        for key, position in sorted(self._positions.items()):
            mark = marks.get(key)
            if mark is None:
                marked.append((key[0], key[1], None, None))
                continue
            mark = self._amount(mark, "mark_price")
            pnl = position.quantity * (mark - position.average_entry_price)
            unrealized += pnl
            market_value += position.quantity * mark
            marked.append((key[0], key[1], mark, pnl))
        equity = (
            self.cash + unrealized
            if self.accounting_mode is AccountingMode.LINEAR_DERIVATIVE
            else self.cash + market_value
        )
        return PortfolioSnapshot(
            account_id=self.account_id, currency=self.cost_model.currency,
            cash=self.cash, realized_pnl=sum((p.realized_pnl for p in self._positions.values()), D("0")),
            unrealized_pnl=unrealized, market_value=market_value, equity=equity,
            positions=tuple(self.positions), marks=tuple(marked),
            ledger_sequence=len(self._entries),
        )


@dataclass(frozen=True, slots=True)
class PortfolioSnapshot:
    account_id: str
    currency: str
    cash: Decimal
    realized_pnl: Decimal
    unrealized_pnl: Decimal
    market_value: Decimal
    equity: Decimal
    positions: Tuple[Position, ...]
    marks: Tuple[tuple, ...]
    ledger_sequence: int
