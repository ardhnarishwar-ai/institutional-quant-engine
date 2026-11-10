"""Deterministic in-memory paper order lifecycle and fill simulation."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, replace
from decimal import Decimal
from enum import Enum
from typing import Dict, Optional, Tuple

from .contracts import ExecutionSource, PaperFill, PaperOrder, PaperOrderStatus
from .eligibility import PaperEligibilityGate
from .fill_models import FillReason, MarketSnapshot, TopOfBookFillModel


class SimulationReason(str, Enum):
    ORDER_ACCEPTED = "ORDER_ACCEPTED"
    ORDER_ALREADY_EXISTS = "ORDER_ALREADY_EXISTS"
    DECISION_ORDER_MISMATCH = "DECISION_ORDER_MISMATCH"
    ELIGIBILITY_REJECTED = "ELIGIBILITY_REJECTED"
    ORDER_NOT_ACTIVE = "ORDER_NOT_ACTIVE"
    DUPLICATE_SNAPSHOT = "DUPLICATE_SNAPSHOT"
    FILLED = "FILLED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    NOT_MARKETABLE = "NOT_MARKETABLE"
    FEED_UNHEALTHY = "FEED_UNHEALTHY"
    BOOK_DESYNCHRONIZED = "BOOK_DESYNCHRONIZED"
    INSTRUMENT_MISMATCH = "INSTRUMENT_MISMATCH"
    VENUE_MISMATCH = "VENUE_MISMATCH"
    INVALID_DIRECTION = "INVALID_DIRECTION"
    NO_DISPLAYED_SIZE = "NO_DISPLAYED_SIZE"


@dataclass(frozen=True, slots=True)
class SimulationResult:
    accepted: bool
    order: Optional[PaperOrder]
    fill: Optional[PaperFill]
    reason: SimulationReason
    detail: str = ""


class PaperSimulator:
    """Local-only simulator. It has no broker, network, or order-routing code."""

    EXECUTION_MODE = "DISABLED"
    EXECUTION_SOURCE = ExecutionSource.PAPER_SIMULATOR

    def __init__(
        self,
        fill_model: Optional[TopOfBookFillModel] = None,
        eligibility_gate: Optional[PaperEligibilityGate] = None,
    ) -> None:
        self.fill_model = fill_model or TopOfBookFillModel()
        self.eligibility_gate = eligibility_gate or PaperEligibilityGate()
        self._orders: Dict[str, PaperOrder] = {}
        self._fills: Dict[str, PaperFill] = {}
        self._processed_snapshots: Dict[str, set[str]] = {}

    @property
    def orders(self) -> Tuple[PaperOrder, ...]:
        return tuple(self._orders[key] for key in sorted(self._orders))

    @property
    def fills(self) -> Tuple[PaperFill, ...]:
        return tuple(self._fills[key] for key in sorted(self._fills))

    def submit(self, decision: object, order: PaperOrder) -> SimulationResult:
        if order.order_id in self._orders:
            return SimulationResult(False, self._orders[order.order_id], None, SimulationReason.ORDER_ALREADY_EXISTS)
        if (
            getattr(decision, "decision_id", None) != order.decision_id
            or getattr(decision, "instrument_id", None) != order.instrument_id
            or getattr(decision, "venue", None) != order.venue
        ):
            return SimulationResult(False, None, None, SimulationReason.DECISION_ORDER_MISMATCH)
        if order.status is not PaperOrderStatus.CREATED or order.filled_quantity != Decimal("0"):
            return SimulationResult(False, None, None, SimulationReason.ORDER_NOT_ACTIVE)
        eligibility = self.eligibility_gate.evaluate(
            decision,
            expected_instrument_id=order.instrument_id,
            expected_venue=order.venue,
        )
        if not eligibility.eligible:
            return SimulationResult(
                False, None, None, SimulationReason.ELIGIBILITY_REJECTED,
                detail=eligibility.reason.value,
            )
        active = replace(order, status=PaperOrderStatus.ACTIVE)
        self._orders[order.order_id] = active
        self._processed_snapshots[order.order_id] = set()
        return SimulationResult(True, active, None, SimulationReason.ORDER_ACCEPTED)

    def process_snapshot(self, snapshot: MarketSnapshot) -> Tuple[SimulationResult, ...]:
        results = []
        for order_id in sorted(self._orders):
            order = self._orders[order_id]
            if order.status not in (PaperOrderStatus.ACTIVE, PaperOrderStatus.PARTIALLY_FILLED):
                continue
            seen = self._processed_snapshots.setdefault(order_id, set())
            if snapshot.snapshot_id in seen:
                results.append(SimulationResult(False, order, None, SimulationReason.DUPLICATE_SNAPSHOT))
                continue
            seen.add(snapshot.snapshot_id)

            proposal = self.fill_model.propose(order, snapshot)
            if proposal.price is None or proposal.quantity <= 0:
                reason = SimulationReason(proposal.reason.value)
                results.append(SimulationResult(False, order, None, reason))
                continue

            side = "BUY" if order.direction.upper() == "LONG" else "SELL"
            material = "|".join((
                order.order_id, snapshot.snapshot_id, side,
                str(proposal.price), str(proposal.quantity),
                proposal.model_version, str(snapshot.simulation_timestamp_ns),
            ))
            fill_id = "paper-" + hashlib.sha256(material.encode("utf-8")).hexdigest()[:24]
            fill = PaperFill(
                fill_id=fill_id,
                order_id=order.order_id,
                venue=order.venue,
                instrument_id=order.instrument_id,
                side=side,
                price=proposal.price,
                quantity=proposal.quantity,
                execution_source=self.EXECUTION_SOURCE,
                market_evidence_event_ids=snapshot.event_ids,
                fill_model_version=proposal.model_version,
                exchange_timestamp_ns=snapshot.exchange_timestamp_ns,
                receive_timestamp_ns=snapshot.receive_timestamp_ns,
                simulation_timestamp_ns=snapshot.simulation_timestamp_ns,
            )
            # Fill ID uniqueness is an additional idempotency guard.
            if fill.fill_id in self._fills:
                results.append(SimulationResult(False, order, None, SimulationReason.DUPLICATE_SNAPSHOT))
                continue
            self._fills[fill.fill_id] = fill
            new_filled = order.filled_quantity + proposal.quantity
            status = PaperOrderStatus.FILLED if new_filled >= order.quantity else PaperOrderStatus.PARTIALLY_FILLED
            updated = replace(order, filled_quantity=new_filled, status=status)
            self._orders[order_id] = updated
            reason = SimulationReason.FILLED if status is PaperOrderStatus.FILLED else SimulationReason.PARTIALLY_FILLED
            results.append(SimulationResult(True, updated, fill, reason))
        return tuple(results)

    def cancel(self, order_id: str) -> SimulationResult:
        order = self._orders.get(order_id)
        if order is None or order.status not in (PaperOrderStatus.ACTIVE, PaperOrderStatus.PARTIALLY_FILLED):
            return SimulationResult(False, order, None, SimulationReason.ORDER_NOT_ACTIVE)
        updated = replace(order, status=PaperOrderStatus.CANCELLED)
        self._orders[order_id] = updated
        return SimulationResult(True, updated, None, SimulationReason.ORDER_NOT_ACTIVE, "paper order cancelled")

    def replay(self, snapshots: Tuple[MarketSnapshot, ...]) -> Tuple[SimulationResult, ...]:
        """Replay supplied snapshots in caller-provided order, deterministically."""
        output = []
        for snapshot in snapshots:
            output.extend(self.process_snapshot(snapshot))
        return tuple(output)
