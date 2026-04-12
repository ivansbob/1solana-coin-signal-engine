# DeFi Health and Carry Score

## Overview
The DeFi Health and Carry Score combines fundamental DeFi protocol health with points/restaking carry potential to identify high-conviction carry trades in DeFi tokens.

## Components

### 1. DeFi Health Score
Measures the fundamental health of a DeFi protocol based on:
- TVL trends and growth
- Revenue generation and sustainability  
- Utilization rates
- Smart money flows
- Rotation context (meme vs DeFi dominance)

### 2. Points / Restaking Carry Score
Evaluates future yield potential from:
- Points accrual velocity (7-day vs 30-day points)
- Restaking yield proxy (blended APY adjusted for token inflation)
- Combined carry score normalized to 0-1 range

## Formulas

### Points Velocity
```
PointsVelocity = points_accrued_7d / (points_accrued_30d + 1)
```

### Restaking Yield Proxy  
```
RestakingYieldProxy = blended_apy × (1 - token_inflation_rate)
```

### Normalization
```
PointsVelocityNorm = min(1.0, PointsVelocity / 2.5)
RestakingYieldNorm = min(1.0, max(0, (RestakingYieldProxy - 4) / 12))
```

### Carry Total Score (0-1)
```
CarryTotalScore = 0.55 × PointsVelocityNorm + 0.45 × RestakingYieldNorm
```

## Integration into Total Score
The carry score is integrated into the final TotalScore with a recommended weight of 0.09:
```
TotalScore = BaseScore + ... + 0.09 × CarryTotalScore + ...
```

For pure DeFi tokens, this weight can be increased to 0.14.

## Trading Rules and Gates

### Warning Conditions
- If `CarryTotalScore < 0.35` → warning `low_carry_potential`

### Synergy Bonus
- If `CarryTotalScore ≥ 0.75` AND `DeFiHealthScore ≥ 0.7` → additional +0.12 to TotalScore

## Examples

### High Carry Scenario (Kamino-like)
- Points 7d: 280, Points 30d: 100 → Velocity = 2.77 → Norm = 1.0
- Blended APY: 20%, Inflation: 5% → Yield Proxy = 19% → Norm = 1.0  
- Carry Score = 0.55×1.0 + 0.45×1.0 = 1.0

### Medium Carry Scenario  
- Points 7d: 70, Points 30d: 50 → Velocity = 1.37 → Norm = 0.55
- Blended APY: 12%, Inflation: 3% → Yield Proxy = 11.64% → Norm = 0.64
- Carry Score = 0.55×0.55 + 0.45×0.64 = 0.59

### Low Carry Scenario
- Points 7d: 15, Points 30d: 25 → Velocity = 0.58 → Norm = 0.23
- Blended APY: 6%, Inflation: 2% → Yield Proxy = 5.88% → Norm = 0.16
- Carry Score = 0.55×0.23 + 0.45×0.16 = 0.20

## Data Sources
- Points data: Dune Analytics
- Yield and inflation data: DefiLlama API
- TVL and protocol health: Multiple sources aggregated

## Maintenance
This score should be recalculated hourly to capture points accrual velocity accurately.