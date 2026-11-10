"""Credential container for Angel One SmartAPI WebSocket Streaming 2.0."""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class SmartAPIConfig:
    jwt_token: str = field(repr=False)
    api_key: str = field(repr=False)
    client_code: str
    feed_token: str = field(repr=False)
    websocket_url: str = "wss://smartapisocket.angelone.in/smart-stream"
    heartbeat_interval_seconds: float = 10.0
    open_timeout_seconds: float = 10.0
    close_timeout_seconds: float = 5.0
    max_retries: int = 3
    retry_base_seconds: float = 1.0
    retry_max_seconds: float = 8.0

    def __post_init__(self):
        for name in ("jwt_token", "api_key", "client_code", "feed_token"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{name} is required")
        if not self.websocket_url.startswith("wss://"):
            raise ValueError("websocket_url must use secure wss:// transport")
        if self.heartbeat_interval_seconds <= 0:
            raise ValueError("heartbeat_interval_seconds must be positive")
        if self.open_timeout_seconds <= 0 or self.close_timeout_seconds <= 0:
            raise ValueError("connection timeouts must be positive")
        if self.max_retries < 0:
            raise ValueError("max_retries cannot be negative")
        if self.retry_base_seconds < 0 or self.retry_max_seconds < self.retry_base_seconds:
            raise ValueError("invalid retry backoff bounds")

    def headers(self) -> dict[str, str]:
        """Return required HTTP upgrade headers; never put credentials in URL."""
        return {
            "Authorization": self.jwt_token,
            "x-api-key": self.api_key,
            "x-client-code": self.client_code,
            "x-feed-token": self.feed_token,
        }
