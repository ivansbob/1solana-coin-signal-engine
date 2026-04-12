# DIP Regime and Exit Logic

The **DIP Regime** handles deep drawdowns explicitly decoupling “catching a falling knife” from typical scaling or momentum strategies. It is completely independent from SCALP or TREND regimes and demands strict statistical verification that a bottom is actively forming.

## Classifier Triggers

To successfully classify as a `DIP` candidate, the Engine validates:
1. **Deep Drawdown:** `drawdown_from_local_high_pct >= 0.25` (25%). A standard volatility swing is ignored.
2. **Rebound Initiation:** `rebound_strength_pct >= 0.08` (8%). Without a confirmed bounce out of the trough, entering is purely gambling against downward momentum.
3. **Sell Exhaustion:** `sell_exhaustion_score >= 0.65`. Downward pressure must fundamentally fracture.
4. **Support Reclaimed:** Must natively flip `True`. Buying under resistance zones leads directly to immediate re-dumps.

## Missing Data Handling

Unlike generic trends where some heuristic padding occurs, DIPs are notoriously risky. **Any missing data covering rebounds, exhaustions, or reclaim levels defaults the evaluation to `IGNORE`.** We do not enter optimistic DIPs.

## The Exit Manager (Post-Entry)

Once a token is legally classified as `DIP` and executed, the `exit_manager.py` routine handles active risk management separately from Trailing Stops.

### Invalidation Logic
An active DIP trade is brutally cut (FAST_EXIT / HARD_SL) if:
1. **New Low Printed:** The price crashes below `local_minimum_at_entry`. The core thesis of the trade is permanently destroyed.
2. **Exhaustion Failing:** Selling resumes forcibly dropping `sell_exhaustion_score < 0.40`. 
3. **Time Degradation:** 45 minutes elapse without clearing a minimal 5% threshold bounce. This prevents dead capital locking in stagnant environments.
