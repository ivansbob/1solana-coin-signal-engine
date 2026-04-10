# Volatility Compression & Breakout (PR-083)

Volatility compression marks periods of decreasing true ranges (ATR) that often precede explosive directional momentum. By pairing short-term (5m) and long-term (60m) True Ranges, we derive the **VolCompressionRatio**.

## Formulas

**Ratio = ATR_5m / (ATR_60m + 0.0001)**

Normalizations (`VolCompressionScore`):
- `1.0` if Ratio <= 0.55
- `0.65` if Ratio <= 0.75
- `0.3` if Ratio <= 0.95
- `0.0` otherwise.

## Breakout Confirmation
A breakout is confirmed if the price has moved >8% over the short window immediately resolving the compression. When `VolCompressionScore >= 0.65` and `breakout_confirmed == True`, a synergy bonus of `0.15` is applied to `TotalScore`.

## Interpretation
- **TREND Validation**: Extremely useful for confirming TREND regime entries. Entering a heavily compressed asset right as volume steps up minimizes chopped stops in noisy sideways trading.
- **Microcap Memes**: Can help filter out fake spikes (where ATR does not compress properly but just jumps wildly constantly). If compression didn't occur first, the move's conviction is inherently lower.
