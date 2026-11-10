"""Strict decoder for Angel One SmartAPI WebSocket Streaming 2.0 packets.

All wire integers/floats are little-endian. Price fields are scaled from paise
to INR by PRICE_DIVISOR. The decoder returns transport facts only: it does not
infer aggressor side, dealer position, or exchange sequence semantics.
"""
from __future__ import annotations

from dataclasses import dataclass
import math
import struct
from typing import Optional


MODE_LTP = 1
MODE_QUOTE = 2
MODE_SNAP_QUOTE = 3
MODE_DEPTH = 4

EXCHANGE_TYPES = {
    1: "NSE_CM",
    2: "NSE_FO",
    3: "BSE_CM",
    4: "BSE_FO",
    5: "MCX_FO",
    7: "NCX_FO",
    13: "CDE_FO",
}

PRICE_DIVISOR = 100.0
BASE_HEADER_SIZE = 43
LTP_PACKET_SIZE = 51
QUOTE_PACKET_SIZE = 123
SNAP_QUOTE_PACKET_SIZE = 379
DEPTH_PACKET_SIZE = 443


class SmartAPIDecodeError(ValueError):
    """Malformed, truncated, or unsupported SmartAPI binary packet."""


@dataclass(frozen=True, slots=True)
class BookLevel:
    side: str
    quantity: int
    price: float
    orders: int


@dataclass(frozen=True, slots=True)
class SmartAPIPacket:
    mode: int
    mode_name: str
    exchange_type: int
    exchange_name: str
    token: str
    sequence_number: Optional[int]
    exchange_timestamp_ms: int
    last_traded_price: Optional[float] = None
    last_traded_quantity: Optional[int] = None
    average_traded_price: Optional[float] = None
    volume_traded_today: Optional[int] = None
    total_buy_quantity: Optional[float] = None
    total_sell_quantity: Optional[float] = None
    open_price: Optional[float] = None
    high_price: Optional[float] = None
    low_price: Optional[float] = None
    close_price: Optional[float] = None
    last_traded_timestamp_ms: Optional[int] = None
    open_interest: Optional[int] = None
    open_interest_change_percent: Optional[float] = None
    upper_circuit_limit: Optional[float] = None
    lower_circuit_limit: Optional[float] = None
    week_52_high: Optional[float] = None
    week_52_low: Optional[float] = None
    best_5_bids: tuple[BookLevel, ...] = ()
    best_5_asks: tuple[BookLevel, ...] = ()
    depth_20_bids: tuple[BookLevel, ...] = ()
    depth_20_asks: tuple[BookLevel, ...] = ()
    # SmartAPI depth packet offset 35 contains a provider "packet received"
    # timestamp, not a documented exchange timestamp. Keep it separate.
    provider_packet_received_ms: Optional[int] = None

    @property
    def is_index_sequence_unavailable(self) -> bool:
        return self.sequence_number is None


def _unpack(fmt: str, data: bytes, offset: int, field: str):
    size = struct.calcsize("<" + fmt)
    end = offset + size
    if offset < 0 or end > len(data):
        raise SmartAPIDecodeError(f"truncated field: {field}")
    return struct.unpack_from("<" + fmt, data, offset)[0]


def _scaled(value: int) -> float:
    return value / PRICE_DIVISOR


def _token(raw: bytes) -> str:
    token_bytes = raw.split(b"\x00", 1)[0]
    if not token_bytes:
        raise SmartAPIDecodeError("empty symbol token")
    try:
        token = token_bytes.decode("ascii")
    except UnicodeDecodeError as exc:
        raise SmartAPIDecodeError("symbol token is not ASCII") from exc
    if not token.isdigit():
        raise SmartAPIDecodeError("symbol token must contain only digits")
    return token


def _best_five(data: bytes) -> tuple[tuple[BookLevel, ...], tuple[BookLevel, ...]]:
    if len(data) != 200:
        raise SmartAPIDecodeError("best-five block must be exactly 200 bytes")
    bids: list[BookLevel] = []
    asks: list[BookLevel] = []
    for index in range(10):
        offset = index * 20
        flag = _unpack("H", data, offset, "best-five flag")
        quantity = _unpack("q", data, offset + 2, "best-five quantity")
        price_raw = _unpack("q", data, offset + 10, "best-five price")
        orders = _unpack("H", data, offset + 18, "best-five order count")
        if flag not in (0, 1):
            raise SmartAPIDecodeError(f"invalid best-five side flag: {flag}")
        if quantity < 0 or price_raw < 0:
            raise SmartAPIDecodeError("negative best-five quantity/price")
        level = BookLevel(
            side="BID" if flag == 1 else "ASK",
            quantity=quantity,
            price=_scaled(price_raw),
            orders=orders,
        )
        # Provider payload is expected to contain five entries per side.
        (bids if flag == 1 else asks).append(level)
    if len(bids) != 5 or len(asks) != 5:
        raise SmartAPIDecodeError("best-five block must contain five bids and five asks")
    return tuple(bids), tuple(asks)


def _depth_levels(data: bytes, *, start: int, side: str) -> tuple[BookLevel, ...]:
    levels = []
    for index in range(20):
        offset = start + index * 10
        quantity = _unpack("i", data, offset, f"{side} quantity")
        price_raw = _unpack("i", data, offset + 4, f"{side} price")
        orders = _unpack("h", data, offset + 8, f"{side} order count")
        if quantity < 0 or price_raw < 0 or orders < 0:
            raise SmartAPIDecodeError(f"negative {side} depth field")
        levels.append(BookLevel(side, quantity, _scaled(price_raw), orders))
    return tuple(levels)


def decode_packet(
    data: bytes | bytearray | memoryview, *, index_tokens: frozenset[str] = frozenset()
) -> SmartAPIPacket:
    """Decode one complete SmartAPI binary frame into immutable typed data."""
    if not isinstance(data, (bytes, bytearray, memoryview)):
        raise TypeError("binary packet must be bytes-like")
    packet = bytes(data)
    if len(packet) < BASE_HEADER_SIZE:
        raise SmartAPIDecodeError(
            f"packet too short for common header: {len(packet)} bytes"
        )

    mode = _unpack("B", packet, 0, "subscription mode")
    exchange_type = _unpack("B", packet, 1, "exchange type")
    if mode not in (MODE_LTP, MODE_QUOTE, MODE_SNAP_QUOTE, MODE_DEPTH):
        raise SmartAPIDecodeError(f"unsupported subscription mode: {mode}")
    if exchange_type not in EXCHANGE_TYPES:
        raise SmartAPIDecodeError(f"unsupported exchange type: {exchange_type}")

    token = _token(packet[2:27])
    sequence_wire = _unpack("q", packet, 27, "sequence number")
    timestamp_wire = _unpack("q", packet, 35, "exchange timestamp")
    if timestamp_wire < 0:
        raise SmartAPIDecodeError("negative timestamp")

    expected_size = {
        MODE_LTP: LTP_PACKET_SIZE,
        MODE_QUOTE: QUOTE_PACKET_SIZE,
        MODE_SNAP_QUOTE: SNAP_QUOTE_PACKET_SIZE,
        MODE_DEPTH: DEPTH_PACKET_SIZE,
    }[mode]
    if len(packet) != expected_size:
        raise SmartAPIDecodeError(
            f"mode {mode} packet must be {expected_size} bytes, got {len(packet)}"
        )

    # Index-feed sequence numbers are explicitly unavailable per provider docs.
    sequence = None if token in index_tokens else sequence_wire
    # Index token identifiers are resolved by the caller's instrument map.
    # The decoder does not guess index identity from a numeric token.
    base = dict(
        mode=mode,
        mode_name={1: "LTP", 2: "QUOTE", 3: "SNAP_QUOTE", 4: "DEPTH"}[mode],
        exchange_type=exchange_type,
        exchange_name=EXCHANGE_TYPES[exchange_type],
        token=token,
        sequence_number=sequence,
        exchange_timestamp_ms=timestamp_wire,
    )

    if mode == MODE_LTP:
        ltp = _unpack("q", packet, 43, "last traded price")
        if ltp < 0:
            raise SmartAPIDecodeError("negative last traded price")
        return SmartAPIPacket(**base, last_traded_price=_scaled(ltp))

    if mode == MODE_DEPTH:
        return decode_depth_packet(packet)

    # Quote and Snap Quote share the first 80 bytes of quote fields.
    fields = {
        "last_traded_price": _scaled(_unpack("q", packet, 43, "last traded price")),
        "last_traded_quantity": _unpack("q", packet, 51, "last traded quantity"),
        "average_traded_price": _scaled(_unpack("q", packet, 59, "average traded price")),
        "volume_traded_today": _unpack("q", packet, 67, "volume traded today"),
        "total_buy_quantity": _unpack("d", packet, 75, "total buy quantity"),
        "total_sell_quantity": _unpack("d", packet, 83, "total sell quantity"),
        "open_price": _scaled(_unpack("q", packet, 91, "open price")),
        "high_price": _scaled(_unpack("q", packet, 99, "high price")),
        "low_price": _scaled(_unpack("q", packet, 107, "low price")),
        "close_price": _scaled(_unpack("q", packet, 115, "close price")),
    }
    for name in ("last_traded_price", "average_traded_price", "open_price",
                 "high_price", "low_price", "close_price"):
        if fields[name] < 0:
            raise SmartAPIDecodeError(f"negative {name}")
    for name in ("last_traded_quantity", "volume_traded_today"):
        if fields[name] < 0:
            raise SmartAPIDecodeError(f"negative {name}")
    for name in ("total_buy_quantity", "total_sell_quantity"):
        if not math.isfinite(fields[name]) or fields[name] < 0:
            raise SmartAPIDecodeError(f"invalid {name}")

    if mode == MODE_QUOTE:
        return SmartAPIPacket(**base, **fields)

    fields.update({
        "last_traded_timestamp_ms": _unpack("q", packet, 123, "last traded timestamp"),
        "open_interest": _unpack("q", packet, 131, "open interest"),
        "open_interest_change_percent": _scaled(
            _unpack("q", packet, 139, "open interest change percentage")
        ),
        "upper_circuit_limit": _scaled(_unpack("q", packet, 347, "upper circuit")),
        "lower_circuit_limit": _scaled(_unpack("q", packet, 355, "lower circuit")),
        "week_52_high": _scaled(_unpack("q", packet, 363, "52-week high")),
        "week_52_low": _scaled(_unpack("q", packet, 371, "52-week low")),
    })
    if fields["open_interest"] < 0:
        raise SmartAPIDecodeError("negative open interest")
    bids, asks = _best_five(packet[147:347])
    return SmartAPIPacket(**base, **fields, best_5_bids=bids, best_5_asks=asks)


def decode_depth_packet(
    data: bytes | bytearray | memoryview,
    *,
    index_tokens=(),
) -> SmartAPIPacket:
    # Independent mode-4 parser: no call to decode_packet or best-five parser.
    if not isinstance(data, (bytes, bytearray, memoryview)):
        raise TypeError("binary packet must be bytes-like")
    packet = bytes(data)
    if len(packet) < 1:
        raise SmartAPIDecodeError("truncated packet before subscription mode")
    mode = _unpack("B", packet, 0, "subscription mode")
    if mode != MODE_DEPTH:
        raise SmartAPIDecodeError("expected mode-4 depth packet")
    if len(packet) != DEPTH_PACKET_SIZE:
        raise SmartAPIDecodeError(
            f"mode-4 depth packet must be {DEPTH_PACKET_SIZE} bytes, got {len(packet)}"
        )
    exchange_type = _unpack("B", packet, 1, "exchange type")
    if exchange_type != 1:
        raise SmartAPIDecodeError("SmartAPI 20-level depth supports NSE cash exchange type 1 only")
    token = _token(packet[2:27])
    provider_received_ms = _unpack("q", packet, 35, "provider packet received time")
    if provider_received_ms < 0:
        raise SmartAPIDecodeError("negative provider packet received time")
    bids = _depth_levels(packet, start=43, side="BID")
    asks = _depth_levels(packet, start=243, side="ASK")
    return SmartAPIPacket(
        mode=MODE_DEPTH,
        mode_name="DEPTH",
        exchange_type=exchange_type,
        exchange_name=EXCHANGE_TYPES[exchange_type],
        token=token,
        sequence_number=None,
        exchange_timestamp_ms=0,
        depth_20_bids=bids,
        depth_20_asks=asks,
        provider_packet_received_ms=provider_received_ms,
    )
