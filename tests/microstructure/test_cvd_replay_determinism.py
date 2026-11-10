from adapters.base.events import EventEnvelope, EventType, TradeEvent
from adapters.base.instrument import InstrumentId, InstrumentKind
from core.microstructure.cvd_calculator import verify_cvd_replay


def make_events():
    inst = InstrumentId(venue="NSE", symbol="RELIANCE", kind=InstrumentKind.NSE_SPOT, quote_currency="INR")
    rows = [("e1", "BUY", 10), ("e2", "SELL", 3), ("e3", "BUY", 5), ("e4", "SELL", 2)]
    return [
        EventEnvelope(
            venue="NSE", instrument=inst, event_type=EventType.TRADE,
            exchange_timestamp_ns=1_000 + i, receive_timestamp_ns=2_000 + i,
            sequence_number=i + 1, sequence_domain="NSE:TRADES:RELIANCE",
            payload=TradeEvent(price=2500, quantity=qty, aggressor_side=side),
            event_id=event_id,
        )
        for i, (event_id, side, qty) in enumerate(rows)
    ]


def test_repeated_full_replay_has_identical_final_state():
    events = make_events()
    outputs = [verify_cvd_replay(events, rolling_window=3) for _ in range(5)]
    assert all(item == outputs[0] for item in outputs)
    final = outputs[0].final_results[0]
    assert final.buy_volume == 15
    assert final.sell_volume == 5
    assert final.cumulative_delta == 10
    assert final.rolling_delta == 0  # last three deltas: -3 + 5 - 2
