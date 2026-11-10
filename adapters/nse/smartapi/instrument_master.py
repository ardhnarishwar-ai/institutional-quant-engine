from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
import math
from typing import Iterable, Mapping
from adapters.base.instrument import InstrumentId, InstrumentKind

class InstrumentMasterError(ValueError):
    pass

def _expiry(value: str) -> str:
    for fmt in ("%d%b%Y", "%Y-%m-%d", "%d-%b-%Y"):
        try:
            return datetime.strptime(value.strip().upper(), fmt).date().isoformat()
        except ValueError:
            pass
    raise InstrumentMasterError(f"unsupported or missing expiry: {value!r}")

def _strike(value) -> float:
    try:
        n = float(value)
    except (TypeError, ValueError) as exc:
        raise InstrumentMasterError(f"invalid strike: {value!r}") from exc
    if not math.isfinite(n) or n <= 0:
        raise InstrumentMasterError("option strike must be finite and positive")
    return n / 100.0

@dataclass(frozen=True, slots=True)
class SmartAPIInstrument:
    exchange_type: int
    token: str
    trading_symbol: str
    instrument: InstrumentId
    lot_size: int = 1
    tick_size: float = 0.05

class SmartAPIInstrumentMaster:
    # Explicit exchange-type/token mapping; never infer an instrument from token text.
    def __init__(self, records: Iterable[Mapping]):
        self._by_token = {}
        self._by_canonical = {}
        for raw in records:
            item = self._normalize(raw)
            key = (item.exchange_type, item.token)
            if key in self._by_token:
                raise InstrumentMasterError(f"duplicate SmartAPI token mapping: {key}")
            if item.instrument.canonical_id in self._by_canonical:
                raise InstrumentMasterError(f"duplicate canonical instrument: {item.instrument.canonical_id}")
            self._by_token[key] = item
            self._by_canonical[item.instrument.canonical_id] = item

    @staticmethod
    def _normalize(raw: Mapping) -> SmartAPIInstrument:
        try:
            segment = str(raw["exch_seg"]).upper()
            token = str(raw["token"]).strip()
            symbol = str(raw.get("name") or raw["symbol"]).strip().upper()
            trading = str(raw.get("symbol") or symbol).strip().upper()
            typ = str(raw.get("instrumenttype") or "").upper()
            if not token.isdigit() or not symbol:
                raise InstrumentMasterError("numeric token and symbol are required")
            if segment == "NSE":
                inst = InstrumentId("NSE", InstrumentKind.NSE_SPOT, symbol, "INR")
                ex_type = 1
            elif segment == "NFO":
                ex_type = 2
                expiry = _expiry(str(raw.get("expiry") or ""))
                if typ in ("FUTIDX", "FUTSTK"):
                    inst = InstrumentId("NSE", InstrumentKind.NSE_FUTURE, symbol, "INR", expiry=expiry)
                elif typ in ("OPTIDX", "OPTSTK"):
                    option = str(raw.get("optiontype") or "").upper()
                    if option not in ("CE", "PE"):
                        option = "CE" if trading.endswith("CE") else ("PE" if trading.endswith("PE") else "")
                    inst = InstrumentId("NSE", InstrumentKind.NSE_OPTION, symbol, "INR",
                                        expiry=expiry, strike=_strike(raw.get("strike")), option_type=option)
                else:
                    raise InstrumentMasterError(f"unsupported NFO instrumenttype: {typ!r}")
            else:
                raise InstrumentMasterError(f"unsupported exchange segment: {segment!r}")
            lot = int(raw.get("lotsize") or 1)
            tick = float(raw.get("tick_size") or raw.get("ticksize") or 0.05)
            if lot <= 0 or not math.isfinite(tick) or tick <= 0:
                raise InstrumentMasterError("lot_size and tick_size must be positive")
            return SmartAPIInstrument(ex_type, token, trading, inst, lot, tick)
        except (KeyError, TypeError, ValueError) as exc:
            if isinstance(exc, InstrumentMasterError):
                raise
            raise InstrumentMasterError(f"invalid scrip-master record: {raw!r}") from exc

    def resolve(self, exchange_type: int, token: str) -> SmartAPIInstrument:
        try:
            return self._by_token[(int(exchange_type), str(token))]
        except KeyError as exc:
            raise InstrumentMasterError(f"unknown SmartAPI token: exchange={exchange_type}, token={token}") from exc

    def __len__(self):
        return len(self._by_token)
