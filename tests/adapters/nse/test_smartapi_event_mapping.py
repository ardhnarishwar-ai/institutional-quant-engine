import json
import pytest
from adapters.base.events import EventType, QuoteEvent, DepthSnapshotEvent
from adapters.nse.smartapi.binary_decoder import BookLevel, SmartAPIPacket
from adapters.nse.smartapi.instrument_master import SmartAPIInstrumentMaster, InstrumentMasterError
from adapters.nse.smartapi.event_mapper import SmartAPIEventMapper
from pipeline.event_journal import EventJournal
from pipeline.event_validator import EventValidator

@pytest.fixture
def master():
    return SmartAPIInstrumentMaster([
        {"exch_seg":"NSE","token":"26000","symbol":"NIFTY"},
        {"exch_seg":"NFO","token":"501","symbol":"NIFTY","expiry":"26NOV2026","instrumenttype":"FUTIDX","lotsize":"65"},
        {"exch_seg":"NFO","token":"502","symbol":"NIFTY","expiry":"26NOV2026","instrumenttype":"OPTIDX","optiontype":"CE","strike":"2500000"},
    ])

def quote_packet(mode=1, exchange=1, token="26000"):
    return SmartAPIPacket(mode,"LTP",exchange,"NSE_CM" if exchange == 1 else "NSE_FO",
        token,44,1700000000123,last_traded_price=22500.0,last_traded_quantity=2,
        average_traded_price=22490.0,volume_traded_today=1234,
        total_buy_quantity=100.0,total_sell_quantity=90.0)

def test_maps_nse_cash_future_option(master):
    assert master.resolve(1,"26000").instrument.canonical_id == "NSE:NSE_SPOT:NIFTY:INR"
    fut=master.resolve(2,"501").instrument
    opt=master.resolve(2,"502").instrument
    assert fut.expiry == "2026-11-26" and fut.kind.value == "NSE_FUTURE"
    assert opt.expiry == "2026-11-26" and opt.strike == 25000 and opt.option_type == "CE"

def test_unknown_token_fails_closed(master):
    with pytest.raises(InstrumentMasterError,match="unknown SmartAPI"):
        master.resolve(1,"999999")

def test_duplicate_token_rejected():
    row={"exch_seg":"NSE","token":"1","symbol":"ABC"}
    with pytest.raises(InstrumentMasterError,match="duplicate"):
        SmartAPIInstrumentMaster([row,row])

def test_quote_is_not_mislabelled_trade(master):
    event=SmartAPIEventMapper(master).map_packet(quote_packet(),receive_timestamp_ns=1700000000999000000)
    assert event.event_type == EventType.QUOTE and isinstance(event.payload,QuoteEvent)
    assert event.exchange_timestamp_ns == 1700000000123000000
    assert event.receive_timestamp_ns == 1700000000999000000
    assert event.sequence_domain == "SMARTAPI:1:26000"

def test_nfo_future_maps(master):
    event=SmartAPIEventMapper(master).map_packet(quote_packet(exchange=2,token="501"),receive_timestamp_ns=123)
    assert event.instrument.kind.value == "NSE_FUTURE"

def test_depth_clock_is_not_promoted_to_exchange_clock(master):
    pkt=SmartAPIPacket(4,"DEPTH",1,"NSE_CM","26000",None,0,
        depth_20_bids=(BookLevel("BID",10,22499.0,2),),
        depth_20_asks=(BookLevel("ASK",12,22501.0,3),),provider_packet_received_ms=1700000000123)
    event=SmartAPIEventMapper(master).map_packet(pkt,receive_timestamp_ns=1700000000999000000)
    assert event.event_type == EventType.DEPTH_SNAPSHOT and isinstance(event.payload,DepthSnapshotEvent)
    assert event.exchange_timestamp_ns is None
    assert event.receive_timestamp_ns == 1700000000999000000
    assert event.sequence_number is None

def test_none_exchange_time_journal_roundtrip_and_validator(tmp_path,master):
    pkt=SmartAPIPacket(4,"DEPTH",1,"NSE_CM","26000",None,0,
        depth_20_bids=(BookLevel("BID",10,22499.0,2),),
        depth_20_asks=(BookLevel("ASK",12,22501.0,3),))
    event=SmartAPIEventMapper(master).map_packet(pkt,receive_timestamp_ns=999)
    journal=EventJournal(tmp_path/"events.jsonl")
    journal.append(event)
    record=json.loads((tmp_path/"events.jsonl").read_text().splitlines()[0])
    restored=journal.decode_event(record)
    assert restored.exchange_timestamp_ns is None and restored.receive_timestamp_ns == 999
    assert EventValidator().validate(restored).accepted

def test_receive_timestamp_must_be_explicit(master):
    with pytest.raises(ValueError,match="receive_timestamp_ns"):
        SmartAPIEventMapper(master).map_packet(quote_packet(),receive_timestamp_ns=-1)
