# Metric Catalog

The absolute source-of-truth identifying execution thresholds securely mapping internal boundaries intuitively for Human Operators.

## Categories of Trust 
To ensure traders discount blind variables intuitively, metrics carry explicit classes:
1. **`execution_grade`**: Hard execution boundaries physically checked by native sources (Liquidity slippage over Jupiter Quotes, native Sybil purity, actual bundle histories). The model NEVER overrides failures on execution grade limits natively.
2. **`research_grade`**: Heavily tested structural boundaries (like `wallet_risk`). Provides immense statistical boundaries avoiding gambling clusters.
3. **`heuristic`**: Abstract representations scoring combinations effectively exclusively (e.g. `SocialVelocity`). Must be heavily discounted natively manually avoiding blind assumptions mapping limits intuitively over operations.

## Catalog Implementations
We enforce complete documentation tracking exactly how limits intuitively scale combinations natively mapping into JSON layers securely enforcing missing degradation natively. Refer to `metric_catalog.py` for exact dictionary layouts.

### OrderflowPurityScore
**Trust Level**: `execution_grade`  
**Unit**: `0..1`  
**Directionality**: `higher_is_better`  
**Description**: Measures the purity of on-chain trading volume by detecting wash-trading, ghost bids, and sybil activity. Filters out artificially inflated volumes common in Solana meme coins.  
**Source**: Dune Analytics (dex_solana.trades) + Helius transaction parsing  
**Interpretation**: Scores >0.8 indicate clean organic flow. Scores <0.4 trigger hard blockers for dirty orderflow. Weight 0.13 in TotalScore V7.

### CumulativeDeltaScore
**Trust Level**: `execution_grade`  
**Unit**: `0..1`  
**Directionality**: `higher_is_better`  
**Description**: Detects hidden accumulation or distribution by measuring divergence between cumulative buy/sell volume and price movement over 24h. Identifies smart money positioning before visible price action.  
**Source**: Dune Analytics (dex_solana.trades) - verified on-chain volume only  
**Interpretation**: Score 1.0 = strong hidden accumulation (divergence ≥0.18), 0.65 = moderate accumulation (0.08-0.18), 0.3 = weak accumulation (0-0.08), 0.0 = hidden distribution. Scores <0.3 warn of potential dumps. Synergizes with VolCompressionScore ≥0.7 for +0.1 bonus. Weight 0.12 in TotalScore V7.

### NarrativeVelocityScore
**Trust Level**: `heuristic`  
**Unit**: `0..1`  
**Directionality**: `higher_is_better`  
**Description**: Measures acceleration of social mentions from X (Twitter) and Telegram platforms over short (5m) vs long (60m) windows. Catches genuine narrative momentum before price spikes, filtering artificial noise.  
**Source**: X API + Telegram Bot API (via OpenClaw adapter)  
**Interpretation**: Score 1.0 = explosive narrative growth (acceleration ≥3.0), 0.65 = strong growth (1.8-3.0), 0.3 = steady growth (1.2-1.8), 0.0 = fading/no growth. Scores <0.3 indicate low narrative support and may warn of weak community engagement. Weight 0.10 in TotalScore V7.

### PointsRestakingCarryScore
**Trust Level**: `research_grade`  
**Unit**: `0..1`  
**Directionality**: `higher_is_better`  
**Description**: Evaluates future yield potential from points accrual velocity and restaking yields adjusted for token inflation. Identifies high-conviction carry trades in DeFi tokens with strong points programs and sustainable yield sources.  
**Source**: Dune Analytics (points data) + DefiLlama API (TVL, APY, emissions)  
**Interpretation**: Score 1.0 = exceptional carry potential (points velocity >=2.5 and yield proxy >=16%), 0.65 = strong carry (velocity >=1.25 and yield proxy >=10%), 0.3 = moderate carry (velocity >=0.5 and yield proxy >=6%), 0.0 = minimal carry. Scores <0.35 trigger low_carry_potential warning. Weight 0.09 in TotalScore V7 (increases to 0.14 for pure DeFi tokens).

### VolCompressionScore
**Trust Level**: `heuristic`
**Unit**: `0..1`
**Directionality**: `higher_is_better`
**Description**: Evaluates volatility compression (ATR) prior to a strong move. Identifies tightening of ranges indicative of an impending major breakout, cutting out noise spikes. Synergizes strongly with actual breakout confirmations.
**Source**: Dune Analytics (dex_solana.trades 1-minute OHLCV aggregations)
**Interpretation**: Score 1.0 = strong compression (ratio <=0.55), 0.65 = steady compression (<=0.75), 0.3 = weak compression (<=0.95), 0.0 = expansion. If breakout is confirmed (>8% over 15m), an additional synergy bonus is applied. Scores <0.3 trigger low_vol_compression warning. Weight 0.11 in TotalScore.

### LiquidityRefillScore
**Trust Level**: `execution_grade`
**Unit**: `0..1`
**Directionality**: `higher_is_better`
**Description**: Measures the time (in seconds) it takes for liquidity to return to 80% of its peak after a sharp spike or dump. Filters out "dead" pools that bleed liquidity permanently.
**Source**: Dune Analytics (dex_solana.pools)
**Interpretation**: Score 1.0 = fast refill (30s <= time <= 180s), 0.6 = moderate refill (180s < time <= 300s), 0.0 = dead pool or extremely slow. Scores <0.4 immediately block the execution with `liquidity_refill_too_slow`. Weight 0.09 in TotalScore.

### HolderChurnScore
**Trust Level**: `research_grade`
**Unit**: `0..1`
**Directionality**: `higher_is_better`
**Description**: Evaluates the proportion of returning vs new buyers over 24 hours. A low churn rate indicates sticky holders and strong continuation. High churn marks massive short-term flipping and dump risks.
**Source**: Dune Analytics (dex_solana.trades)
**Interpretation**: Score 1.0 = highly sticky (>=65% returning), 0.5 = mixed (40-65% returning), 0.0 = massive flippers (<40% returning). High churn (>60% / returning < 40%) triggers a `high_holder_churn` execution warning. Weight 0.11 in TotalScore.

### WalletLeadLagScore
**Trust Level**: `research_grade`
**Unit**: `0..1`
**Directionality**: `higher_is_better`
**Description**: Measures the temporal difference (lead-lag) between buys from highly profitable smart wallets (leaders) and regular wallets (followers). Identifies genuine "chains" of smart money and confirms signals across multiple timeframes.
**Source**: Dune Analytics (dex_solana.trades)
**Interpretation**: Score 1.0 = strong lead-lag (8-45 seconds), 0.65 = moderate lag (45-90s), 0.3 = weak lag (90-180s), 0.0 = sybil (<8s) or too slow (>180s). Scores <0.4 trigger a `weak_lead_lag` warning. If Score >= 0.8 and SmartMoney >= 0.7, adds +0.12 synergy bonus. Weight 0.11 in TotalScore. (Multi-Timeframe Confirmation adds up to +0.08).
