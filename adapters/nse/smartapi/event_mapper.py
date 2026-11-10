from __future__ import annotations
from adapters.base.events import DepthSnapshotEvent, DepthSnapshotLevel, EventEnvelope, EventType, QuoteEvent
from adapters.nse.smartapi.binary_decoder import SmartAPIPacket
from adapters.nse.smartapi.instrument_master import SmartAPIInstrumentMaster

class SmartAPIEventMapper:
    # No clock reads or inferred aggressor classifications in this mapper.
    def __init__(self, instrument_master: SmartAPIInstrumentMaster):
        self.master = instrument_master

    def map_packet(self, packet: SmartAPIPacket, *, receive_timestamp_ns: int,
                   event_id: str | None = None) -> EventEnvelope:
        if not isinstance(receive_timestamp_ns, int) or isinstance(receive_timestamp_ns, bool) or receive_timestamp_ns < 0:
            raise ValueError("receive_timestamp_ns must be a non-negative integer")
        item = self.master.resolve(packet.exchange_type, packet.token)
        if packet.mode == 4:
            bids = tuple(sorted(
                (DepthSnapshotLevel(x.price, x.quantity) for x in packet.depth_20_bids if x.price > 0 and x.quantity > 0),
                key=lambda x: x.price, reverse=True))
            asks = tuple(sorted(
                (DepthSnapshotLevel(x.price, x.quantity) for x in packet.depth_20_asks if x.price > 0 and x.quantity > 0),
                key=lambda x: x.price))
            payload = DepthSnapshotEvent(bids=bids, asks=asks, snapshot_sequence=None)
            typ, exchange_ns, sequence, domain = EventType.DEPTH_SNAPSHOT, None, None, None
        elif packet.mode in (1, 2, 3):
            payload = QuoteEvent(
                last_price=packet.last_traded_price, last_quantity=packet.last_traded_quantity,
                cumulative_volume=packet.volume_traded_today, average_price=packet.average_traded_price,
                open_price=packet.open_price, high_price=packet.high_price, low_price=packet.low_price,
                close_price=packet.close_price, total_buy_quantity=packet.total_buy_quantity,
                total_sell_quantity=packet.total_sell_quantity, open_interest=packet.open_interest)
            typ = EventType.QUOTE
            exchange_ns = packet.exchange_timestamp_ms * 1_000_000
            sequence = packet.sequence_number
            domain = f"SMARTAPI:{packet.exchange_type}:{packet.token}" if sequence is not None else None
        else:
            raise ValueError(f"unsupported SmartAPI mode: {packet.mode}")
        stamp = exchange_ns if exchange_ns is not None else receive_timestamp_ns
        seq_id = sequence if sequence is not None else "noseq"
        event_id = event_id or f"smartapi:{packet.exchange_type}:{packet.token}:{packet.mode}:{stamp}:{seq_id}"
        return EventEnvelope(venue="NSE", instrument=item.instrument, event_type=typ,
            exchange_timestamp_ns=exchange_ns, receive_timestamp_ns=receive_timestamp_ns,
            sequence_number=sequence, sequence_domain=domain, payload=payload, event_id=event_id)
