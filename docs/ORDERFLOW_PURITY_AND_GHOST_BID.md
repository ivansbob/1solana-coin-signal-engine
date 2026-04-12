# Orderflow Purity and Ghost Bid Score

## Overview

The **Orderflow Purity Score** is a critical metric for evaluating the quality of on-chain trading volume in Solana tokens. It specifically targets the rampant issues of wash-trading, ghost bids, and artificial volume inflation that plague many meme coins and low-quality projects.

## Problem Statement

Solana's high throughput and low fees make it susceptible to:
- **Wash Trading**: Bots trading between controlled wallets to inflate volume
- **Ghost Bids**: Failed transactions or unfilled limit orders that appear as activity
- **Sybil Clusters**: Coordinated buying from related wallets
- **Artificial Pumps**: Bots creating false liquidity signals

These manipulations severely degrade signal quality, leading to false positives in trading algorithms.

## Metric Components

### 1. Ghost Bid Ratio
```sql
GhostBidRatio = number_of_ghost_bids_or_failed_tx / total_tx_in_window
```

- **Ghost Bids**: Transactions that fail or orders that don't execute
- **Detection**: Via Helius transaction parsing
- **Threshold**: < 0.08 for clean flow

### 2. Wash Trade Proxy
```sql
WashTradeProxy = volume_from_repeated_wallet_pairs / total_volume
```

- **Repeated Pairs**: Wallets trading back and forth multiple times
- **Time Window**: Within 5 minutes
- **Threshold**: < 0.12 for clean flow

### 3. Organic Buy Ratio
```sql
OrganicBuyRatio = unique_organic_buyers / total_buyers
```

- **Organic Buyers**: Excluding wash trade and ghost bid participants
- **Uniqueness**: Distinct wallets with genuine patterns
- **Threshold**: ≥ 0.65 for clean flow

## Purity Score Formula

```python
if GhostBidRatio ≤ 0.08 and WashTradeProxy ≤ 0.12 and OrganicBuyRatio ≥ 0.65:
    OrderflowPurityScore = 1.0
elif GhostBidRatio ≤ 0.15 and WashTradeProxy ≤ 0.25:
    OrderflowPurityScore = 0.6
elif GhostBidRatio > 0.25 or WashTradeProxy > 0.35:
    OrderflowPurityScore = 0.2
else:
    OrderflowPurityScore = 0.0
```

## Integration in TotalScore V7

- **Weight**: 0.13 (13% contribution)
- **Hard Gate**: Blocks execution if < 0.4 ("dirty_orderflow")
- **Synergy Bonus**: +0.1 if PurityScore ≥ 0.8 and SmartWalletCoordScore ≥ 0.7

## Impact on Signal Quality

### Clean Flow (Score ≥ 0.8)
- High confidence in volume-based signals
- Reliable momentum indicators
- Lower risk of manipulation-driven pumps

### Moderate Noise (Score 0.6)
- Acceptable for trending assets
- Monitor closely for degradation
- Discount volume signals by 20-30%

### Dirty Flow (Score ≤ 0.2)
- Hard block execution
- Indicates severe manipulation
- Avoid all volume-based strategies

## Technical Implementation

### Data Sources
- **Dune Analytics**: `dex_solana.trades` for transaction data
- **Helius**: Transaction parsing for failed/ghost bids
- **Time Window**: Default 60 minutes

### SQL Query Structure
```sql
-- See queries/dune/orderflow_purity_ghost_bid.sql
-- CTEs for: token_trades, ghost_bids, wash_trades, organic_buyers, total_stats
```

### Testing Scenarios
1. **Clean Organic Flow**: ghost=0.04, wash=0.08, organic=0.78 → score=1.0
2. **Moderate Noise**: ghost=0.12, wash=0.18 → score=0.6
3. **Wash Trading Heavy**: wash=0.42 → score=0.2
4. **Ghost Bid Heavy**: ghost=0.31 → score=0.0

## Future Enhancements

- Real-time monitoring dashboards
- Adaptive thresholds based on token age/market cap
- Integration with cross-chain volume validation
- Machine learning models for advanced manipulation detection