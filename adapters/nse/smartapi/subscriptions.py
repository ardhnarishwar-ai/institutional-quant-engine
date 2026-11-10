"""Validated SmartAPI Streaming 2.0 subscription request models."""
from __future__ import annotations

from dataclasses import dataclass
import re

VALID_MODES = {1, 2, 3, 4}
VALID_EXCHANGE_TYPES = {1, 2, 3, 4, 5, 7, 13}
MAX_SESSION_SUBSCRIPTIONS = 1000
MAX_DEPTH_TOKENS = 50
_TOKEN = re.compile(r"^[0-9]{1,25}$")


@dataclass(frozen=True, slots=True)
class ExchangeTokens:
    exchange_type: int
    tokens: tuple[str, ...]

    def __post_init__(self):
        if self.exchange_type not in VALID_EXCHANGE_TYPES:
            raise ValueError(f"unsupported SmartAPI exchange type: {self.exchange_type}")
        if not self.tokens:
            raise ValueError("tokens cannot be empty")
        if len(set(self.tokens)) != len(self.tokens):
            raise ValueError("duplicate tokens in one exchange group")
        if any(not isinstance(token, str) or not _TOKEN.fullmatch(token) for token in self.tokens):
            raise ValueError("tokens must be numeric strings of 1 to 25 digits")


@dataclass(frozen=True, slots=True)
class Subscription:
    mode: int
    token_groups: tuple[ExchangeTokens, ...]

    def __post_init__(self):
        if self.mode not in VALID_MODES:
            raise ValueError("mode must be 1 (LTP), 2 (Quote), 3 (Snap Quote), or 4 (Depth)")
        if not self.token_groups:
            raise ValueError("at least one token group is required")
        if not all(isinstance(group, ExchangeTokens) for group in self.token_groups):
            raise TypeError("token_groups must contain ExchangeTokens")
        exchanges = [group.exchange_type for group in self.token_groups]
        if len(set(exchanges)) != len(exchanges):
            raise ValueError("combine tokens for each exchange into one group")
        if self.mode == 4:
            if any(group.exchange_type != 1 for group in self.token_groups):
                raise ValueError("SmartAPI depth mode supports NSE cash exchange type 1 only")
            if sum(len(group.tokens) for group in self.token_groups) > MAX_DEPTH_TOKENS:
                raise ValueError(f"depth mode is limited to {MAX_DEPTH_TOKENS} tokens")

    @property
    def subscription_count(self) -> int:
        return sum(len(group.tokens) for group in self.token_groups)

    def payload(self, *, action: int, correlation_id: str | None = None) -> dict:
        if action not in (0, 1):
            raise ValueError("action must be 1 (subscribe) or 0 (unsubscribe)")
        result = {
            "action": action,
            "params": {
                "mode": self.mode,
                "tokenList": [
                    {"exchangeType": group.exchange_type, "tokens": list(group.tokens)}
                    for group in self.token_groups
                ],
            },
        }
        if correlation_id is not None:
            if not re.fullmatch(r"[A-Za-z0-9]{1,10}", correlation_id):
                raise ValueError("correlation_id must be 1-10 alphanumeric characters")
            result["correlationID"] = correlation_id
        return result
