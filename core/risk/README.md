# Stage 5B.3 — Risk & Decision Gate

- Consumes an upstream FusionResult without modifying it.
- Uses hard vetoes plus a weighted soft-risk score.
- Missing liquidity, VPIN, GEX-age, market-risk, or account-risk inputs fail closed to HOLD.
- Feed, book, provenance, stale-signal, instrument, history and breached account limits are hard vetoes.
- Separate exchange and receive timestamps are retained; no cross-clock subtraction is performed.
- `DecisionEvent.execution_mode` is permanently `DISABLED`.
- This stage does not connect to a broker, construct orders, or execute trades.

`RiskContext` is an explicit integration boundary. The caller must supply measurements from trusted upstream systems. Missing values remain missing and are not interpreted as zero risk.
