# Orderflow Purity and Delta Divergence

## Overview
This document covers the integration of **Cumulative Delta Divergence** into the orderflow analysis pipeline, complementing existing purity metrics to detect sophisticated accumulation/distribution patterns.

## Cumulative Delta Divergence

### Formula
\[
\text{CumDeltaDiv} = \frac{\text{cum_buy_volume}_{24h} - \text{cum_sell_volume}_{24h}}{\text{price_change_bps}_{24h} + 1}
\]

### Score Normalization
| Divergence Range | Score | Interpretation |
|------------------|-------|---------------|
| ≥ 0.18 | 1.0 | Strong hidden accumulation |
| 0.08 - 0.18 | 0.65 | Moderate accumulation |
| 0.0 - 0.08 | 0.3 | Weak accumulation |
| < 0.0 | 0.0 | Hidden distribution |

### Signal Strength
- **TREND**: High scores (≥0.65) indicate smart money positioning for upward moves
- **SCALP**: Neutral/weak scores help identify low-risk scalp opportunities
- **DIP**: Low scores (<0.3) warn of potential distribution before rebounds

### Synergy Effects
- When `CumulativeDeltaScore ≥ 0.65` AND `VolCompressionScore ≥ 0.7`: +0.1 bonus to TotalScore
- Warning gate: `CumulativeDeltaScore < 0.3` triggers `negative_delta_divergence` flag

### Data Source
- **Table**: `dex_solana.trades` (Dune Analytics)
- **Filter**: `verified = true` (on-chain confirmed trades only)
- **Window**: 24 hours rolling
- **Confidence**: High (direct on-chain volume measurement)

### Examples

#### Strong Accumulation Signal
```
Token: SOLANA_MONKE
Buy Volume: $500K | Sell Volume: $200K | Price Change: +2.5%
Divergence: (500000 - 200000) / (250 + 1) = 0.992
Score: 1.0 → BULLISH TREND SIGNAL
```

#### Hidden Distribution Warning
```
Token: RISKY_MEME
Buy Volume: $50K | Sell Volume: $80K | Price Change: +1.0%
Divergence: (50000 - 80000) / (100 + 1) = -0.297
Score: 0.0 → DISTRIBUTION WARNING (avoid DIP trades)
```

#### Neutral Flow
```
Token: STABLE_COIN
Buy Volume: $1.2M | Sell Volume: $1.1M | Price Change: +0.5%
Divergence: (1200000 - 1100000) / (50 + 1) = 0.0198
Score: 0.3 → NEUTRAL (safe for SCALP)
```

## Integration Notes
- Computed alongside `OrderflowPurityScore` in scoring pipeline
- Weight: 0.12 in TotalScore V7
- Fails gracefully with `None` values for missing data
- Zero price change handled with +1 denominator to prevent division errors