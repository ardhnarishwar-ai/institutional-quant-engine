from adapters.base.events import EventEnvelope, EventType, TradeEvent
from adapters.base.instrument import InstrumentId, InstrumentKind
from core.microstructure.volume_profile import VolumeProfile


def recorded_fixture():
    instrument = InstrumentId(venue="NSE", symbol="RELIANCE",
                             kind=InstrumentKind.NSE_SPOT, quote_currency="INR")
    rows = [
        ("r1", 100.0, 10.0, "BUY"),
        ("r2", 100.0, 4.0, "SELL"),
        ("r3", 100.5, 8.0, "BUY"),
        ("r4", 101.0, 12.0, "SELL"),
        ("r5", 100.5, 3.0, "BUY"),
    ]
    return [
        EventEnvelope(
            venue="NSE", instrument=instrument, event_type=EventType.TRADE,
            exchange_timestamp_ns=1_000_000 + i, receive_timestamp_ns=2_000_000 + i,
            sequence_number=i + 1, sequence_domain="NSE:TRADES:RELIANCE",
            payload=TradeEvent(price=price, quantity=qty, aggressor_side=side),
            event_id=event_id,
        ) for i, (event_id, price, qty, side) in enumerate(rows)
    ]


def replay(events):
    profile = VolumeProfile(tick_size=0.5, rolling_window=3, session_id="fixture-session")
    for event in events:
        profile.update(event)
    return profile.session_snapshot(), profile.rolling_snapshot()


def test_repeated_recorded_fixture_replay_is_identical():
    events = recorded_fixture()
    first = replay(events)
    second = replay(iter(events))
    third = replay(events)
    assert first == second == third
    session, rolling = first
    assert session.total_volume == 37
    assert session.buy_volume == 21
    assert session.sell_volume == 16
    assert rolling.total_volume == 23
    assert rolling.event_count == 3
