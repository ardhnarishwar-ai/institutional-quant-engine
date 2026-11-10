# Market Gravity OS — audited microstructure baseline

This repository baseline contains independent calculations and state models:
- Dealer-sign-conditioned Black–Scholes gamma exposure (GEX)
- Tick-level CVD / price-displacement absorption candidates
- Fixed-volume VPIN buckets with empirical toxicity rank
- L2 market-by-price depletion/refill reconstruction
- Timestamped aggregate market state with stale-feed and skew flags

## Install and test (Termux/Linux)
```sh
python -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements.txt
python -m pytest -q
```

## Important data limitations
1. Open interest alone does not reveal dealer inventory. GEX requires an
   externally supplied dealer position sign per option row; output is
   conditional on that assumption/source.
2. Absorption output is a candidate only. Aggressor side must be classified
   upstream; L2 replenishment evidence is needed to strengthen the inference.
3. VPIN is sensitive to trade-side classification and volume-bucket choice.
   Tick-rule fallback is not the full quote-based Lee–Ready algorithm.
4. L2 market-by-price updates cannot identify individual hidden orders or
   prove iceberg activity. Exchange sequence numbers, snapshot recovery,
   duplicate handling and feed-specific normalization belong in adapters.
5. No broker adapter or live order execution is included or enabled.

The modules are intentionally exchange-agnostic. Validate each venue's feed
semantics and replay against timestamped historical data before production use.
