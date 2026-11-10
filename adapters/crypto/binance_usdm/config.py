from dataclasses import dataclass
@dataclass(frozen=True, slots=True)
class BinanceUSDMConfig:
    symbol: str = "BTCUSDT"
    base_url: str = "wss://fstream.binance.com/stream"
    depth_speed: str = "100ms"
    depth_limit: int = 1000
    reconnect_max_retries: int = 5
    reconnect_base_seconds: float = 0.5
    reconnect_max_seconds: float = 8.0
    def __post_init__(self):
        if self.symbol != "BTCUSDT": raise ValueError("BTCUSDT perpetual only")
        if not self.base_url.startswith("wss://"): raise ValueError("secure wss:// required")
        if self.depth_speed not in ("100ms", "500ms"): raise ValueError("invalid depth speed")
        if self.depth_limit not in (5,10,20,50,100,500,1000): raise ValueError("invalid depth limit")
        if self.reconnect_max_retries < 0 or self.reconnect_base_seconds < 0: raise ValueError("invalid retry policy")
        if self.reconnect_max_seconds < self.reconnect_base_seconds: raise ValueError("invalid max backoff")
    @property
    def streams(self):
        s=self.symbol.lower()
        return (f"{s}@aggTrade", f"{s}@depth@{self.depth_speed}")
    @property
    def websocket_url(self):
        return self.base_url+"?streams="+"/".join(self.streams)
