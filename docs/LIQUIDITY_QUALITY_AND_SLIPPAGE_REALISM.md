# Liquidity Quality and Slippage Realism

Tokens artificially inflating their MarketCap through non-standard bonding curves or heavily illiquid LP deposits display massive structural slippage on trivial execution volumes. The `Liquidity Quality` algorithm decouples base nominal values tracked by DexScreener from practical execution impact metrics supplied directly by Jupiter.

## Asymmetric Market Reality

The bot inherently trades against `Ask` models while competing with `Bid` liquidity. Asymmetric exit structures allow simple entry (1-5 bps slippage) but punish exits dramatically (50-100 bps). 
By computing `SellImpactNorm` distinctly alongside baseline structural compositions like `DynamicLiquidityShare`, the framework strictly evaluates whether the user realizes profit logically.

## Gate Upgrades
1. **`excessive_buy_impact`**: A mathematically constrained hard blocker eliminating tokens featuring Jupiter Buy Impact > 45 bps.
2. **`dangerous_sell_slippage`**: Notifies exit strategies when liquidity exits punish returns heavily (>90 bps).
3. **`fragile_liquidity`**: A soft blocker flagging tokens comprised densely of dynamic structures (>65% dynamic) scoring an overall low impact resilience. 
4. **`liquidity_refill_too_slow`**: A hard blocker filtering pools where `LiquidityRefillScore < 0.4`. Discards "dead" pools that bleed out completely after a spike.

Missing route data is NOT an optimistic buy setup. Missing parameter properties deliberately force massive limits (like default buy impacts of 100 bsp) triggering immediate `excessive_buy_impact` blockers, keeping operations secure.

## Liquidity Refill Half-Life
Liquidity on heavily trending and volatile pairs will periodically spike or dump relative to entry volumes. Instead of only relying on absolute static thresholds, we measure the **Refill Half-Life**: the time (in seconds) it takes to organically return to 80% of peak liquidity. 
- Fast Refills (30-180s) indicate healthy market-maker reloading and yield perfect multiplier scores (+0.09). 
- Dead pools (where liquidity never recovers) yield a 0.0 modifier and cause immediate execution blocking.
