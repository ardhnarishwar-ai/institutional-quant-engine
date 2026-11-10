# Institutional Quant Engine — microstructure core

Exchange-agnostic research core for observable market microstructure:
- Dealer-sign-conditioned option gamma exposure (GEX)
- Tick-level CVD / price-displacement absorption candidates
- Fixed-volume VPIN buckets and historical toxicity rank
- L2 market-by-price depletion/replenishment reconstruction
- Timestamped aggregate market state with feed consistency checks

## Install and test (Termux/Linux)
```sh
python -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements.txt
python -m pytest -q
```

## Data and model limitations
1. Open interest alone does not reveal dealer inventory. GEX requires an
   externally supplied dealer position sign per option row.
2. Absorption output is a candidate. Aggressor side must be classified
   upstream; L2 replenishment is supporting evidence, not proof of an iceberg.
3. VPIN depends on trade-side classification and bucket size. Tick-rule
   fallback is not the full quote-based Lee–Ready algorithm.
4. L2 market-by-price data cannot identify individual hidden orders.
   Exchange sequence recovery and feed normalization belong in adapters.
5. No broker adapter or live order execution is included or enabled.

## Commit 2: core hardening and deterministic replay
Order-book depth reductions are counted as executed depletion only when
matched to trade prints at the same passive-side price within
`max_event_age_ms`. Unmatched cancellations are not executions. Replenishment
is counted only after matched execution depletion. Losing the defended best
price permanently invalidates that candidate.

VPIN toxicity rank is a prior-only empirical mid-rank percentile of the
current rolling VPIN against historical rolling VPIN observations. The current
observation enters the reference distribution only after its rank is computed.

Market-state aggregation enforces strictly increasing per-component sequence
numbers when supplied. Cross-feed sequence values are compared only when
components explicitly share a `sequence_domain`; unrelated venue sequence
spaces are not compared. Event-time skew is checked independently.

GitHub Actions runs pytest on Python 3.10, 3.11 and 3.12.
