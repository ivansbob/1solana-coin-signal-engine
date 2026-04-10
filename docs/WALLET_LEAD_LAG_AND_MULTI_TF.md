# Wallet Lead-Lag and Multi-Timeframe Confirmation

## Overview

Wallet Lead-Lag detects temporal precedence in smart wallet buying patterns, identifying "true alpha" chains where high-winrate wallets lead market movements. Combined with Multi-Timeframe Confirmation, it validates signals across short-term horizons to reduce false positives in TREND and SCALP regimes.

## Core Concept

Smart wallets with historical win-rates ≥65% often buy first, followed by less sophisticated followers. This creates measurable lag times that correlate with successful entries.

## Formulas

### LeadLagSec
Average time difference between leader wallet buys and subsequent follower buys within a 60-minute window.

Leaders: wallets with win-rate ≥65% (from Dune wallet_win_rate_cache)

Followers: all other wallets

### LeadLagScore
```
if 8 ≤ LeadLagSec ≤ 45: 1.0
elif 45 < LeadLagSec ≤ 90: 0.65
elif 90 < LeadLagSec ≤ 180: 0.3
else: 0.0  # Too fast (sybil-like) or too slow
```

### Multi-Timeframe Confirmation Score
Validates activity persistence across timeframes:
```
if confirmed on 1m + 5m + 15m: 1.0
if confirmed on 1m + 5m only: 0.6
if confirmed on 1m only: 0.2
else: 0.0
```

## Integration in TotalScore

- LeadLagScore weight: 0.11
- MultiTFScore weight: 0.08
- Synergy bonus: +0.12 if LeadLagScore ≥0.8 AND SmartMoneyCombinedScore ≥0.7

## Gates
- Soft blocker "weak_lead_lag" if LeadLagScore < 0.4

## Examples
- Strong signal: 22s lag + 3/3 TF → 1.0 score
- Moderate: 68s lag → 0.65
- Sybil: 2s lag → 0.0 (too fast)
- Weak: 140s lag → 0.3

## Impact on Signals
Significantly improves TREND accuracy by confirming genuine smart money flows, reducing chases of exhausted pumps.</content>
<parameter name="filePath">docs/WALLET_LEAD_LAG_AND_MULTI_TF.md