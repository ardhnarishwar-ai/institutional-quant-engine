import struct

import pytest

from adapters.nse.smartapi.binary_decoder import (
    DEPTH_PACKET_SIZE, LTP_PACKET_SIZE, QUOTE_PACKET_SIZE,
    SNAP_QUOTE_PACKET_SIZE, SmartAPIDecodeError,
    decode_depth_packet, decode_packet,
)


def put(fmt, value, offset, packet):
    struct.pack_into("<" + fmt, packet, offset, value)


def header(mode, *, exchange=2, token="99926000", sequence=123456, timestamp=1700000000123):
    packet = bytearray(43)
    put("B", mode, 0, packet)
    put("B", exchange, 1, packet)
    token_bytes = token.encode("ascii")
    packet[2:27] = token_bytes[:25].ljust(25, b"\x00")
    put("q", sequence, 27, packet)
    put("q", timestamp, 35, packet)
    return packet


def ltp_packet(*, exchange=2):
    packet = header(1, exchange=exchange) + bytearray(8)
    put("q", 225675, 43, packet)
    return bytes(packet)


def quote_packet(mode=2):
    size = QUOTE_PACKET_SIZE if mode == 2 else SNAP_QUOTE_PACKET_SIZE
    packet = header(mode) + bytearray(size - 43)
    values = [
        (43, 225675), (51, 12), (59, 224000), (67, 100000),
        (75, 1200.5), (83, 950.25), (91, 220000), (99, 230000),
        (107, 215000), (115, 219000),
    ]
    for offset, value in values:
        put("d" if offset in (75, 83) else "q", value, offset, packet)
    if mode == 3:
        put("q", 1700000000000, 123, packet)
        put("q", 50000, 131, packet)
        put("q", 250, 139, packet)
        for i in range(10):
            off = 147 + i * 20
            flag = 1 if i < 5 else 0
            put("H", flag, off, packet)
            put("q", 100 + i, off + 2, packet)
            put("q", 225000 + i * 100, off + 10, packet)
            put("H", 2 + i, off + 18, packet)
        for off, val in [(347, 250000), (355, 200000), (363, 275000), (371, 175000)]:
            put("q", val, off, packet)
    return bytes(packet)


def depth_packet():
    packet = header(4, exchange=1) + bytearray(DEPTH_PACKET_SIZE - 43)
    for i in range(20):
        bid = 43 + i * 10
        ask = 243 + i * 10
        for off, qty, price, orders in (
            (bid, 1000 + i, 225000 - i * 100, 3 + i),
            (ask, 2000 + i, 225100 + i * 100, 4 + i),
        ):
            put("i", qty, off, packet)
            put("i", price, off + 4, packet)
            put("h", orders, off + 8, packet)
    return bytes(packet)


def test_decode_ltp_fields_and_paise_scaling():
    decoded = decode_packet(ltp_packet())
    assert len(ltp_packet()) == LTP_PACKET_SIZE
    assert decoded.mode_name == "LTP"
    assert decoded.exchange_name == "NSE_FO"
    assert decoded.token == "99926000"
    assert decoded.sequence_number == 123456
    assert decoded.last_traded_price == 2256.75


def test_decode_quote_mode_fields():
    decoded = decode_packet(quote_packet(2))
    assert len(quote_packet(2)) == QUOTE_PACKET_SIZE
    assert decoded.last_traded_price == 2256.75
    assert decoded.last_traded_quantity == 12
    assert decoded.average_traded_price == 2240.0
    assert decoded.volume_traded_today == 100000
    assert decoded.total_buy_quantity == 1200.5
    assert decoded.open_price == 2200.0
    assert decoded.close_price == 2190.0


def test_decode_snap_quote_best_five_and_circuit_limits():
    decoded = decode_packet(quote_packet(3))
    assert len(quote_packet(3)) == SNAP_QUOTE_PACKET_SIZE
    assert decoded.open_interest == 50000
    assert decoded.upper_circuit_limit == 2500.0
    assert decoded.lower_circuit_limit == 2000.0
    assert len(decoded.best_5_bids) == 5
    assert len(decoded.best_5_asks) == 5
    assert decoded.best_5_bids[0].side == "BID"
    assert decoded.best_5_asks[0].side == "ASK"


def test_decode_depth_twenty_levels_keeps_provider_timestamp_distinct():
    raw = depth_packet()
    assert len(raw) == DEPTH_PACKET_SIZE
    decoded = decode_depth_packet(raw)
    assert decoded.sequence_number is None
    assert decoded.exchange_timestamp_ms == 0
    assert decoded.provider_packet_received_ms == 1700000000123
    assert len(decoded.depth_20_bids) == 20
    assert len(decoded.depth_20_asks) == 20
    assert decoded.depth_20_bids[0].price == 2250.0
    assert decoded.depth_20_asks[0].quantity == 2000


def test_depth_decoder_rejects_non_depth_mode():
    with pytest.raises(SmartAPIDecodeError, match="expected mode-4"):
        decode_depth_packet(ltp_packet())


@pytest.mark.parametrize("raw", [b"", b"\x01", bytes(42)])
def test_rejects_truncated_common_header(raw):
    with pytest.raises(SmartAPIDecodeError):
        decode_packet(raw)


@pytest.mark.parametrize("mode,size", [(1, 50), (2, 122), (3, 378), (4, 442)])
def test_rejects_truncated_mode_packet(mode, size):
    packet = header(mode) + bytearray(max(0, size - 43))
    with pytest.raises(SmartAPIDecodeError):
        decode_packet(bytes(packet))


def test_rejects_unknown_mode_and_exchange():
    packet = bytearray(ltp_packet())
    packet[0] = 9
    with pytest.raises(SmartAPIDecodeError, match="subscription mode"):
        decode_packet(packet)
    packet = bytearray(ltp_packet())
    packet[1] = 99
    with pytest.raises(SmartAPIDecodeError, match="exchange type"):
        decode_packet(packet)


def test_rejects_invalid_or_empty_token():
    packet = bytearray(ltp_packet())
    packet[2:27] = b"\x00" * 25
    with pytest.raises(SmartAPIDecodeError, match="empty"):
        decode_packet(packet)
    packet[2:27] = b"12A\x00" + b"\x00" * 21
    with pytest.raises(SmartAPIDecodeError, match="only digits"):
        decode_packet(packet)


def test_index_feed_sequence_is_suppressed_only_when_instrument_master_marks_token():
    # Index classification belongs to the instrument master, not exchange id alone.
    decoded = decode_packet(ltp_packet(exchange=1), index_tokens=frozenset({"99926000"}))
    assert decoded.sequence_number is None
    ordinary = decode_packet(ltp_packet(exchange=1))
    assert ordinary.sequence_number == 123456
