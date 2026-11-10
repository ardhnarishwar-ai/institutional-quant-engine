'''Unified instrument-isolated microstructure analytics orchestration.

Only explicit BUY/SELL canonical trades feed directional CVD, VPIN and
absorption. Genuine trades with unknown aggressor side are recorded separately
and remain unknown; they never enter VPIN/absorption and are never guessed.
GEX is refreshed separately using an explicit option chain, spot, dealer signs
and as_of_timestamp_ns. Exchange and receive clocks remain independent.
'''
from __future__ import annotations
from dataclasses import dataclass
from typing import Mapping, Optional

from adapters.base.events import EventEnvelope, EventType, TradeEvent
from core.microstructure.absorption_divergence import AbsorptionConfig, AbsorptionDetector, AbsorptionSignal
from core.microstructure.cvd_calculator import CVDCalculator, CVDResult
from core.microstructure.gex_calculator import GEXCalculator, GEXResult
from core.microstructure.volume_profile import VolumeProfile, ProfileSnapshot
from core.microstructure.vpin_calculator import VPINCalculator, VPINConfig, VPINResult


class AnalyticsStateError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class AnalyticsState:
    venue: str
    instrument_id: str
    session_id: str
    event_count: int
    classified_trade_count: int
    unknown_trade_count: int
    ignored_non_trade_count: int
    exchange_timestamp_ns: Optional[int]
    receive_timestamp_ns: Optional[int]
    last_event_id: Optional[str]
    cvd: Optional[CVDResult]
    volume_profile: ProfileSnapshot
    rolling_volume_profile: ProfileSnapshot
    vpin: Optional[VPINResult]
    absorption: Optional[AbsorptionSignal]
    gex: Optional[GEXResult]
    gex_as_of_timestamp_ns: Optional[int]
    dealer_positioning_assumption: Optional[str]
    audit_sequence: int
    flags: tuple[str, ...]


class InstrumentAnalytics:
    def __init__(self, *, venue: str, instrument_id: str, tick_size: float,
                 session_id: str = "default", cvd_rolling_window: int = 1000,
                 profile_rolling_window: int = 1000, value_area_percentage: float = 70.0,
                 vpin_config: Optional[VPINConfig] = None,
                 absorption_config: AbsorptionConfig = AbsorptionConfig()):
        if not venue or not instrument_id or not session_id:
            raise ValueError("venue, instrument_id and session_id are required")
        self.venue, self.instrument_id, self.session_id = venue.upper(), instrument_id, session_id
        self.cvd = CVDCalculator(rolling_window=cvd_rolling_window, session_id=session_id)
        self.profile = VolumeProfile(tick_size=tick_size, rolling_window=profile_rolling_window,
                                     value_area_percentage=value_area_percentage, session_id=session_id)
        self._vpin_config = vpin_config
        self._absorption_config = absorption_config
        self.vpin = VPINCalculator(vpin_config) if vpin_config is not None else None
        self.absorption_detector = AbsorptionDetector(absorption_config)
        self._cvd = self._vpin = self._absorption = self._gex = None
        self._gex_as_of_ns = self._exchange_ns = self._receive_ns = None
        self._last_event_id = None
        self._seen_ids = set()
        self._event_count = self._classified_count = self._unknown_count = self._ignored_count = 0
        self._audit_sequence = 0
        self._flags = []

    def _identity(self, event):
        if event.venue != self.venue or event.instrument.canonical_id != self.instrument_id:
            raise AnalyticsStateError("event does not belong to this instrument state")

    def _time(self, exchange_ns, receive_ns):
        if exchange_ns is not None:
            if not isinstance(exchange_ns, int) or isinstance(exchange_ns, bool) or exchange_ns < 0:
                raise AnalyticsStateError("exchange_timestamp_ns must be None or non-negative int")
            if self._exchange_ns is not None and exchange_ns < self._exchange_ns:
                raise AnalyticsStateError("exchange timestamp regressed")
        if not isinstance(receive_ns, int) or isinstance(receive_ns, bool) or receive_ns < 0:
            raise AnalyticsStateError("receive_timestamp_ns must be non-negative int")
        if self._receive_ns is not None and receive_ns < self._receive_ns:
            raise AnalyticsStateError("receive timestamp regressed")

    def _audit(self, event_id, exchange_ns, receive_ns):
        self._event_count += 1
        self._audit_sequence += 1
        self._last_event_id = event_id
        self._seen_ids.add(event_id)
        if exchange_ns is not None:
            self._exchange_ns = exchange_ns
        self._receive_ns = receive_ns

    def update(self, event: EventEnvelope, *, reference_level: Optional[float] = None):
        if not isinstance(event, EventEnvelope):
            raise TypeError("event must be EventEnvelope")
        self._identity(event)
        if event.event_id in self._seen_ids:
            raise AnalyticsStateError(f"duplicate event_id: {event.event_id}")
        self._time(event.exchange_timestamp_ns, event.receive_timestamp_ns)
        if event.event_type != EventType.TRADE:
            self._audit(event.event_id, event.exchange_timestamp_ns, event.receive_timestamp_ns)
            self._ignored_count += 1
            self._flags.append(f"IGNORED_NON_TRADE:{event.event_type.value}")
            return self.snapshot()
        if not isinstance(event.payload, TradeEvent):
            raise AnalyticsStateError("TRADE event must carry TradeEvent payload")
        trade = event.payload
        self._cvd = self.cvd.update(event)
        self.profile.update(event)
        if self.vpin is not None:
            # Explicit side only: never call VPIN with side=None/tick-rule fallback.
            self._vpin = self.vpin.update(event.exchange_timestamp_ns, trade.quantity,
                                          side=trade.aggressor_side, price=trade.price)
        self._absorption = self.absorption_detector.update(
            event.exchange_timestamp_ns, trade.price, trade.quantity,
            trade.aggressor_side, reference_level,
        )
        self._classified_count += 1
        self._audit(event.event_id, event.exchange_timestamp_ns, event.receive_timestamp_ns)
        return self.snapshot()

    def record_unclassified_trade(self, *, event_id: str, price: float, quantity: float,
                                  exchange_timestamp_ns: Optional[int], receive_timestamp_ns: int):
        if not isinstance(event_id, str) or not event_id.strip():
            raise ValueError("event_id is required")
        if event_id in self._seen_ids:
            raise AnalyticsStateError(f"duplicate event_id: {event_id}")
        self._time(exchange_timestamp_ns, receive_timestamp_ns)
        self._cvd = self.cvd.record_unclassified_trade(
            event_id=event_id, exchange_timestamp_ns=exchange_timestamp_ns,
            receive_timestamp_ns=receive_timestamp_ns, quantity=quantity)
        self.profile.record_unclassified_trade(
            venue=self.venue, instrument_id=self.instrument_id, event_id=event_id,
            price=price, quantity=quantity, exchange_timestamp_ns=exchange_timestamp_ns,
            receive_timestamp_ns=receive_timestamp_ns)
        self._unknown_count += 1
        self._flags.append("UNKNOWN_AGGRESSOR_SIDE_PRESERVED")
        self._audit(event_id, exchange_timestamp_ns, receive_timestamp_ns)
        return self.snapshot()

    def update_gex(self, options, *, spot: float, as_of_timestamp_ns: int,
                   rate: float = 0.0, dividend_yield: float = 0.0,
                   grid_points: int = 401, grid_width: float = 0.20):
        if not isinstance(as_of_timestamp_ns, int) or isinstance(as_of_timestamp_ns, bool) or as_of_timestamp_ns < 0:
            raise ValueError("GEX as_of_timestamp_ns must be a non-negative integer")
        if self._exchange_ns is not None and as_of_timestamp_ns > self._exchange_ns:
            raise AnalyticsStateError("GEX as_of timestamp cannot be later than latest exchange event time")
        self._gex = GEXCalculator().calculate(
            options, spot, rate=rate, dividend_yield=dividend_yield,
            grid_points=grid_points, grid_width=grid_width,
            as_of_timestamp_ns=as_of_timestamp_ns)
        self._gex_as_of_ns = as_of_timestamp_ns
        return self._gex

    def start_session(self, session_id: str):
        if not isinstance(session_id, str) or not session_id.strip():
            raise ValueError("session_id is required")
        if session_id == self.session_id:
            raise AnalyticsStateError("new session_id must differ from current session")
        self.cvd.start_session(session_id)
        self.profile.start_session(session_id)
        self.session_id = session_id
        self._event_count = self._classified_count = self._unknown_count = self._ignored_count = 0
        self._exchange_ns = self._receive_ns = self._last_event_id = None
        self._cvd = self._vpin = self._absorption = self._gex = None
        self.vpin = VPINCalculator(self._vpin_config) if self._vpin_config is not None else None
        self.absorption_detector = AbsorptionDetector(self._absorption_config)
        self._gex_as_of_ns = None
        self._flags.clear()
        # Dedupe history and audit sequence persist across sessions.

    def snapshot(self):
        return AnalyticsState(
            self.venue, self.instrument_id, self.session_id, self._event_count,
            self._classified_count, self._unknown_count, self._ignored_count,
            self._exchange_ns, self._receive_ns, self._last_event_id, self._cvd,
            self.profile.session_snapshot(), self.profile.rolling_snapshot(),
            self._vpin, self._absorption, self._gex, self._gex_as_of_ns,
            self._gex.dealer_positioning_assumption if self._gex else None,
            self._audit_sequence, tuple(self._flags))


class AnalyticsRegistry:
    def __init__(self, *, tick_size_by_instrument: Mapping[str, float],
                 session_id: str = "default", cvd_rolling_window: int = 1000,
                 profile_rolling_window: int = 1000, value_area_percentage: float = 70.0,
                 vpin_config: Optional[VPINConfig] = None,
                 absorption_config: AbsorptionConfig = AbsorptionConfig()):
        self.tick_size_by_instrument = dict(tick_size_by_instrument)
        self.kwargs = dict(session_id=session_id, cvd_rolling_window=cvd_rolling_window,
                           profile_rolling_window=profile_rolling_window,
                           value_area_percentage=value_area_percentage,
                           vpin_config=vpin_config, absorption_config=absorption_config)
        self._states = {}

    def state_for(self, venue: str, instrument_id: str):
        key = (venue.upper(), instrument_id)
        if key not in self._states:
            if instrument_id not in self.tick_size_by_instrument:
                raise AnalyticsStateError(f"tick size not configured for {instrument_id}")
            self._states[key] = InstrumentAnalytics(
                venue=key[0], instrument_id=instrument_id,
                tick_size=self.tick_size_by_instrument[instrument_id], **self.kwargs)
        return self._states[key]

    def update(self, event: EventEnvelope, *, reference_level: Optional[float] = None):
        return self.state_for(event.venue, event.instrument.canonical_id).update(
            event, reference_level=reference_level)

    def snapshots(self):
        return tuple(self._states[k].snapshot() for k in sorted(self._states))
