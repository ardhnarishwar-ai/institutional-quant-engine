"""Stage 5C.1 paper-trading orchestration.

This module is an explicit downstream coordinator. It does not modify or
monkey-patch LiveIngestPipeline, adapters, signal, risk, simulator or ledger.
Call on_accepted_event only after LiveIngestPipeline has accepted and applied
the event to AnalyticsRegistry. Contexts, prices, orders and account inputs
are caller-supplied; this module never fabricates them.
"""
from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass
from decimal import Decimal
from typing import Callable, Mapping, Optional

from adapters.base.events import EventEnvelope
from core.microstructure.analytics_state import AnalyticsRegistry
from core.signals.evidence import normalize_analytics_state
from core.signals.fusion import FusionContext, MicrostructureFusionEngine, FusionResult
from core.risk.decision_gate import DecisionGate
from core.risk.engine import RiskContext, RiskResult
from core.paper.contracts import PaperOrder
from core.paper.fill_models import MarketSnapshot
from core.paper.simulator import PaperSimulator, SimulationResult
from core.paper.ledger import PaperPortfolioLedger, ApplyFillResult


@dataclass(frozen=True, slots=True)
class PaperCycleResult:
    event_id: str
    venue: str
    instrument_id: str
    analytics_audit_sequence: int
    fusion: FusionResult
    risk: Optional[RiskResult]
    simulation: Optional[SimulationResult]
    source_event_ids: tuple[str, ...]
    status: str


@dataclass(frozen=True, slots=True)
class PaperFillCycleResult:
    snapshot_id: str
    simulation_results: tuple[SimulationResult, ...]
    ledger_results: tuple[ApplyFillResult, ...]


class PaperTradingOrchestrator:
    """Coordinates accepted analytics events through fusion, risk and paper accounting.

    Required mappings are keyed by canonical instrument ID. Context providers
    receive (event, analytics_state) and must return the exact typed contexts.
    order_factory receives (DecisionEvent, EventEnvelope) and returns a
    PaperOrder or None. No order is created by default.
    """

    def __init__(
        self,
        *,
        analytics: AnalyticsRegistry,
        fusion_engines: Mapping[str, MicrostructureFusionEngine],
        risk_gates: Mapping[str, DecisionGate],
        fusion_context_provider: Callable[[EventEnvelope, object], FusionContext],
        risk_context_provider: Callable[[EventEnvelope, object, FusionResult], RiskContext],
        reference_price_provider: Callable[[EventEnvelope, object], Optional[float]],
        paper_simulator: PaperSimulator,
        ledger: PaperPortfolioLedger,
        order_factory: Optional[Callable[[object, EventEnvelope], Optional[PaperOrder]]] = None,
        provenance_window: int = 256,
    ) -> None:
        if not callable(getattr(analytics, "state_for", None)):
            raise TypeError("analytics must expose AnalyticsRegistry.state_for()")
        if not isinstance(paper_simulator, PaperSimulator):
            raise TypeError("paper_simulator must be PaperSimulator")
        if not isinstance(ledger, PaperPortfolioLedger):
            raise TypeError("ledger must be PaperPortfolioLedger")
        if not isinstance(provenance_window, int) or isinstance(provenance_window, bool) or provenance_window < 1:
            raise ValueError("provenance_window must be a positive integer")
        for name, provider in (
            ("fusion_context_provider", fusion_context_provider),
            ("risk_context_provider", risk_context_provider),
            ("reference_price_provider", reference_price_provider),
        ):
            if not callable(provider):
                raise TypeError(f"{name} must be callable")
        self.analytics = analytics
        self.fusion_engines = dict(fusion_engines)
        self.risk_gates = dict(risk_gates)
        self.fusion_context_provider = fusion_context_provider
        self.risk_context_provider = risk_context_provider
        self.reference_price_provider = reference_price_provider
        self.paper_simulator = paper_simulator
        self.ledger = ledger
        self.order_factory = order_factory
        self.provenance_window = provenance_window
        self._event_ids = defaultdict(lambda: deque(maxlen=provenance_window))
        self._seen_event_ids = set()
        self._seen_snapshot_ids = set()
        self._decision_ids = set()
        self._fill_ids = set()

    def on_accepted_event(self, event: EventEnvelope, *, source_event_ids=None) -> PaperCycleResult:
        """Process an event already accepted by LiveIngestPipeline.

        The registry must already contain this event (last_event_id check).
        Source IDs may be supplied by the caller from its journal cursor; absent
        that, this instance accumulates accepted IDs in observed call order.
        """
        if not isinstance(event, EventEnvelope):
            raise TypeError("event must be EventEnvelope")
        if event.event_id in self._seen_event_ids:
            raise ValueError(f"duplicate orchestrator event: {event.event_id}")
        instrument_id = event.instrument.canonical_id
        state = self.analytics.state_for(event.venue, instrument_id).snapshot()
        if state.last_event_id != event.event_id:
            raise ValueError(
                "event is not the latest accepted analytics event; call only after "
                "LiveIngestPipeline has accepted and applied it"
            )
        key = (event.venue.upper(), instrument_id)
        self._seen_event_ids.add(event.event_id)
        self._event_ids[key].append(event.event_id)
        ids = tuple(source_event_ids) if source_event_ids is not None else tuple(self._event_ids[key])
        if not ids or event.event_id not in ids or len(set(ids)) != len(ids):
            raise ValueError("source_event_ids must be unique genuine IDs and include current event")
        if any(not isinstance(x, str) or not x.strip() for x in ids):
            raise ValueError("source_event_ids must contain non-empty strings")

        fusion_engine = self.fusion_engines.get(instrument_id)
        risk_gate = self.risk_gates.get(instrument_id)
        if fusion_engine is None or risk_gate is None:
            raise KeyError(f"fusion and risk policies must be configured for {instrument_id}")

        reference_price = self.reference_price_provider(event, state)
        bundle = normalize_analytics_state(
            state,
            source_event_ids=ids,
            reference_price=reference_price,
            history_event_count=state.event_count,
            classified_trade_count=state.classified_trade_count,
        )
        fusion_context = self.fusion_context_provider(event, state)
        if not isinstance(fusion_context, FusionContext):
            raise TypeError("fusion_context_provider must return FusionContext")
        fusion = fusion_engine.evaluate(bundle, fusion_context)

        if not fusion.accepted or fusion.signal is None:
            return PaperCycleResult(event.event_id, event.venue, instrument_id,
                state.audit_sequence, fusion, None, None, ids, "FUSION_REJECTED")

        risk_context = self.risk_context_provider(event, state, fusion)
        if not isinstance(risk_context, RiskContext):
            raise TypeError("risk_context_provider must return RiskContext")
        risk = risk_gate.evaluate(fusion, risk_context)
        decision = risk.decision
        status = getattr(decision.status, "value", decision.status)
        if status != "APPROVED":
            return PaperCycleResult(event.event_id, event.venue, instrument_id,
                state.audit_sequence, fusion, risk, None, ids, f"RISK_{status}")

        if self.order_factory is None:
            return PaperCycleResult(event.event_id, event.venue, instrument_id,
                state.audit_sequence, fusion, risk, None, ids, "APPROVED_NO_ORDER_FACTORY")
        order = self.order_factory(decision, event)
        if order is None:
            return PaperCycleResult(event.event_id, event.venue, instrument_id,
                state.audit_sequence, fusion, risk, None, ids, "APPROVED_NO_ORDER")
        if not isinstance(order, PaperOrder):
            raise TypeError("order_factory must return PaperOrder or None")
        if order.decision_id != decision.decision_id:
            raise ValueError("order_factory returned an order for a different decision")
        if decision.decision_id in self._decision_ids:
            raise ValueError(f"duplicate decision submitted: {decision.decision_id}")
        self._decision_ids.add(decision.decision_id)
        simulation = self.paper_simulator.submit(decision, order)
        return PaperCycleResult(event.event_id, event.venue, instrument_id,
            state.audit_sequence, fusion, risk, simulation, ids,
            "ORDER_ACCEPTED" if simulation.accepted else "ORDER_REJECTED")

    def process_snapshot(self, snapshot: MarketSnapshot) -> PaperFillCycleResult:
        """Simulate fills and apply each unique paper fill to the ledger once."""
        if not isinstance(snapshot, MarketSnapshot):
            raise TypeError("snapshot must be MarketSnapshot")
        if snapshot.snapshot_id in self._seen_snapshot_ids:
            raise ValueError(f"duplicate orchestrator snapshot: {snapshot.snapshot_id}")
        self._seen_snapshot_ids.add(snapshot.snapshot_id)
        results = self.paper_simulator.process_snapshot(snapshot)
        ledger_results = []
        for result in results:
            fill = result.fill
            if fill is None:
                continue
            if fill.fill_id in self._fill_ids:
                raise RuntimeError(f"simulator emitted duplicate fill: {fill.fill_id}")
            self._fill_ids.add(fill.fill_id)
            ledger_results.append(self.ledger.apply_fill(fill))
        return PaperFillCycleResult(snapshot.snapshot_id, tuple(results), tuple(ledger_results))
