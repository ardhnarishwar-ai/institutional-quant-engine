"""Stage 5C.3 real pipeline executor: canonical journal -> analytics -> orchestration.

The deterministic input fixtures are canonical EventEnvelope test fixtures,
not claimed exchange-live recordings. Every invocation creates fresh analytics,
fusion, risk, simulator and ledger state. No broker connectivity exists.
"""
from __future__ import annotations
import tempfile
from decimal import Decimal
from pathlib import Path
from typing import Any

from adapters.base.events import EventEnvelope, EventType, TradeEvent
from adapters.base.instrument import InstrumentId, InstrumentKind
from adapters.replay.reader import ReplayReader
from benchmarks.scenario_runner import canonicalize, result_digest
from benchmarks.scenarios.catalog import INSTRUMENTS, SCENARIO_TEMPLATES, build_scenario_matrix
from benchmarks.scenarios.contracts import ScenarioSpec
from core.microstructure.analytics_state import AnalyticsRegistry
from core.paper.contracts import PaperOrder, PaperOrderType
from core.paper.costs import FeeSchedule, TransactionCostModel
from core.paper.fill_models import BookLevel, MarketSnapshot
from core.paper.ledger import AccountingMode, PaperPortfolioLedger
from core.paper.simulator import PaperSimulator
from core.risk.decision_gate import DecisionGate
from core.risk.engine import RiskContext
from core.risk.policy import DEFAULT_RISK_POLICIES
from core.signals.fusion import FusionContext, MicrostructureFusionEngine
from core.signals.fusion_config import DEFAULT_POLICIES, FusionPolicy
from pipeline.event_journal import EventJournal
from pipeline.paper_trading import PaperTradingOrchestrator

TICKS={"NIFTY":0.05,"BANKNIFTY":0.05,"FINNIFTY":0.05,"BTCUSDT":0.1}
CASH=Decimal("1000000")

def instrument_for(spec):
    p=spec.instrument
    kind=InstrumentKind.CRYPTO_PERPETUAL if p.key=="BTCUSDT" else InstrumentKind.NSE_SPOT
    return InstrumentId(venue=p.venue,kind=kind,symbol=p.instrument_id,quote_currency=p.currency)

def base_price(spec):
    return Decimal("60000") if spec.instrument.key=="BTCUSDT" else Decimal("25000")

def fixture_events(spec):
    inst=instrument_for(spec)
    count=12 if spec.template_key=="insufficient_history" else 120
    price=float(base_price(spec))
    result=[]
    for i in range(count):
        side=("SELL" if spec.template_key=="directional_short" else
              ("BUY" if i%2==0 else "SELL") if spec.template_key=="conflicting_evidence" else "BUY")
        result.append(EventEnvelope(
            venue=spec.instrument.venue,instrument=inst,event_type=EventType.TRADE,
            exchange_timestamp_ns=1_700_000_000_000_000_000+i*1_000_000,
            receive_timestamp_ns=1_700_000_000_100_000_000+i*1_000_000,
            sequence_number=i+1,sequence_domain=f"{spec.instrument.venue}:{spec.instrument.key}:TRADE",
            payload=TradeEvent(price=price,quantity=1.0,aggressor_side=side),
            event_id=f"fixture:{spec.scenario_id}:trade:{i:04d}"))
    return tuple(result)

def contexts(spec,event,state):
    stale=spec.template_key=="stale_feed"
    sync=spec.template_key!="desynchronized_book"
    age=3_000_000_000 if stale else 0
    fc=FusionContext("STALE" if stale else "HEALTHY",age,sync,None)
    missing=spec.template_key=="missing_liquidity"
    rc=RiskContext(
        now_receive_timestamp_ns=event.receive_timestamp_ns,
        feed_status="STALE" if stale else "HEALTHY",feed_age_ns=age,
        book_synchronized=sync,history_ready=state.event_count>=100,
        market_risk=0.10,
        liquidity_spread_ticks=None if missing else 0.5,
        liquidity_depth_units=None if missing else 10.0,
        vpin_risk=0.10,gex_age_ns=0,exposure_utilization=0.10,
        daily_loss_utilization=0.10,audit_sequence=state.audit_sequence,
        exchange_reference_timestamp_ns=event.exchange_timestamp_ns)
    return fc,rc

def make_order_factory(spec,last_id):
    def factory(decision,event):
        if event.event_id!=last_id or spec.template_key not in {
            "marketable_limit","partial_depth","non_marketable_limit"}: return None
        px=base_price(spec); direction=str(decision.direction)
        if spec.template_key=="non_marketable_limit":
            limit=px-Decimal("1") if direction=="LONG" else px+Decimal("1")
        else:
            limit=px+Decimal("1") if direction=="LONG" else px-Decimal("1")
        return PaperOrder(
            order_id=f"paper-order:{spec.scenario_id}",decision_id=decision.decision_id,
            venue=spec.instrument.venue,instrument_id=event.instrument.canonical_id,
            direction=direction,order_type=PaperOrderType.MARKETABLE_LIMIT,
            limit_price=limit,quantity=Decimal("1"),
            created_exchange_timestamp_ns=event.exchange_timestamp_ns,
            created_receive_timestamp_ns=event.receive_timestamp_ns,
            created_simulation_timestamp_ns=event.receive_timestamp_ns+1,
            source_event_ids=(event.event_id,))
    return factory

def execute_scenario(spec:ScenarioSpec,run_index:int)->dict[str,Any]:
    if run_index not in (1,2): raise ValueError("run_index must be 1 or 2")
    events=fixture_events(spec); instrument_id=events[0].instrument.canonical_id
    registry=AnalyticsRegistry(tick_size_by_instrument={instrument_id:TICKS[spec.instrument.key]})
    fp=DEFAULT_POLICIES[instrument_id]
    fp=FusionPolicy(
        instrument_id=fp.instrument_id,weights=fp.weights,
        minimum_coverage=fp.minimum_coverage,minimum_absolute_score=fp.minimum_absolute_score,
        minimum_directional_votes=fp.minimum_directional_votes,conflict_tolerance=fp.conflict_tolerance,
        max_vpin_risk_penalty=fp.max_vpin_risk_penalty,max_gex_age_ns=fp.max_gex_age_ns,
        minimum_history_events=100,minimum_classified_trades=20,max_feed_age_ns=fp.max_feed_age_ns)
    fusion=MicrostructureFusionEngine(fp)
    risk=DecisionGate(DEFAULT_RISK_POLICIES[instrument_id])
    ledger=PaperPortfolioLedger(
        f"scenario:{spec.scenario_id}",CASH,
        TransactionCostModel(FeeSchedule(),spec.instrument.currency),
        accounting_mode=AccountingMode.LINEAR_DERIVATIVE)
    simulator=PaperSimulator()
    last_id=events[-1].event_id
    orch=PaperTradingOrchestrator(
        analytics=registry,fusion_engines={instrument_id:fusion},risk_gates={instrument_id:risk},
        fusion_context_provider=lambda e,s:contexts(spec,e,s)[0],
        risk_context_provider=lambda e,s,f:contexts(spec,e,s)[1],
        reference_price_provider=lambda e,s:float(base_price(spec)),
        paper_simulator=simulator,ledger=ledger,
        order_factory=make_order_factory(spec,last_id))
    with tempfile.TemporaryDirectory(prefix="stage5c3-") as tmp:
        path=Path(tmp)/"events.jsonl"; journal=EventJournal(path)
        for event in events: journal.append(event)
        cycles=[]; ids=[]
        for event in ReplayReader(path).events():
            ids.append(event.event_id)
            registry.update(event)
            cycles.append(orch.on_accepted_event(event))
    submitted=any(c.simulation is not None and c.simulation.accepted for c in cycles)
    fill_cycle=None
    if submitted:
        px=base_price(spec); qty=Decimal("0.4") if spec.template_key=="partial_depth" else Decimal("5")
        snap=MarketSnapshot(
            snapshot_id=f"snapshot:{spec.scenario_id}",venue=spec.instrument.venue,
            instrument_id=instrument_id,bids=(BookLevel(px-1,Decimal("5")),),
            asks=(BookLevel(px,qty),),event_ids=(events[-1].event_id,),
            exchange_timestamp_ns=events[-1].exchange_timestamp_ns,
            receive_timestamp_ns=events[-1].receive_timestamp_ns+2,
            simulation_timestamp_ns=events[-1].receive_timestamp_ns+3)
        fill_cycle=orch.process_snapshot(snap)
    state=registry.state_for(spec.instrument.venue,instrument_id).snapshot()
    last=cycles[-1]; final_risk=next((c.risk for c in reversed(cycles) if c.risk is not None),None)
    fills=simulator.fills; entries=ledger.entries
    if ledger.cost_model.currency!=spec.instrument.currency: raise AssertionError("ledger currency mismatch")
    if any(e.costs.currency!=spec.instrument.currency for e in entries): raise AssertionError("ledger entry currency mismatch")
    if any(f.execution_source.value!="PAPER_SIMULATOR" for f in fills): raise AssertionError("non-paper fill")
    if any(c.fusion.signal is not None and c.fusion.signal.execution_mode.value!="DISABLED" for c in cycles):
        raise AssertionError("signal execution mode not disabled")
    if final_risk and final_risk.decision.execution_mode!="DISABLED": raise AssertionError("decision execution mode not disabled")
    snap=ledger.mark_to_market({})
    return {
        "scenario_id":spec.scenario_id,"instrument_id":instrument_id,"venue":spec.instrument.venue,
        "currency":snap.currency,"journal_event_ids":tuple(ids),"journal_event_count":len(ids),
        "analytics":{"audit_sequence":state.audit_sequence,"event_count":state.event_count,
            "classified_trade_count":state.classified_trade_count,"last_event_id":state.last_event_id,
            "exchange_timestamp_ns":state.exchange_timestamp_ns,"receive_timestamp_ns":state.receive_timestamp_ns},
        "fusion":{"accepted":last.fusion.accepted,"direction":getattr(last.fusion.direction,"value",last.fusion.direction),
            "score":last.fusion.score,"coverage":last.fusion.evidence_coverage,
            "signal_id":last.fusion.signal.signal_id if last.fusion.signal else None},
        "risk":{"status":getattr(final_risk.decision.status,"value",final_risk.decision.status) if final_risk else None,
            "score":final_risk.assessment.risk_score if final_risk else None},
        "cycle_statuses":tuple(c.status for c in cycles),
        "orders":canonicalize(simulator.orders),"fills":canonicalize(fills),
        "ledger_entries":canonicalize(entries),"ledger":canonicalize(snap),
        "fill_reasons":tuple(getattr(x.reason,"value",x.reason) for x in fill_cycle.simulation_results) if fill_cycle else (),
        "execution_mode":"DISABLED",
        "execution_source":"PAPER_SIMULATOR" if fills else None,
    }

def run_end_to_end_matrix():
    rows=[]
    for spec in build_scenario_matrix():
        a=execute_scenario(spec,1); b=execute_scenario(spec,2)
        da,db=result_digest(a),result_digest(b)
        if da!=db: raise AssertionError(f"dual-run mismatch {spec.scenario_id}: {da} != {db}")
        rows.append({"scenario_id":spec.scenario_id,"instrument":spec.instrument.key,
            "currency":spec.instrument.currency,"run_1_sha256":da,"run_2_sha256":db,
            "deterministic":True,"observation":a})
    report={"stage":"5C.3","execution_mode":"DISABLED","instrument_count":len(INSTRUMENTS),
        "scenario_template_count":len(SCENARIO_TEMPLATES),"scenario_count":len(rows),"run_count":2,
        "all_deterministic":all(x["deterministic"] for x in rows),
        "currency_by_instrument":{x.key:x.currency for x in INSTRUMENTS},"scenarios":rows}
    report["report_sha256"]=result_digest(report)
    return report
