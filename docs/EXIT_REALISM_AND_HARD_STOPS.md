# Exit Realism & Hard Stops

Relying on nominal API charts showing `+30% PB` completely fails inside high-slippage low-liquidity pairs. 
Exits natively drop *Gross Mark to Market* measurements instead exclusively utilizing calculated Net margins inclusive of Jito bribes and transaction frictions.

## Net Executable Logic 

```math
NetExecutablePnLPct = GrossPnLPct - BuyImpactBps - SellImpactBps - TotalFeeBps
```
The strategy considers you *not actually in profit* until executing backwards through the local pools would deposit more SOL into your network account than initial costs. All TPs scale off the `net_executable_pnl` exclusively.

## Dynamic Stop Types

1. **Unconditional Stop (-18%)**: Automatically rips operations abandoning operations completely once network boundaries cross explicitly outside of 18% mathematical losses. Prevents account drains structurally regardless of expected recoveries.
2. **Regime Aware Stops**: 
 - `SCALP` (-12%): Exits rapidly upon minor degradation avoiding volatile flip overs.
 - `TREND` (-22%): Affords room corresponding to natural local minimum oscillation. 
 - `DIP` (-28%): Requires massive ranges natively since dip executions catch highly volatile downswings dynamically. 

3. **Bagholder Defenses**:
 If distributions structurally indicate deep unbonding behaviors overriding original smart-metric bounds, operations convert to `DEFENSIVE_EXIT` states structurally pulling local stakes away quickly.
