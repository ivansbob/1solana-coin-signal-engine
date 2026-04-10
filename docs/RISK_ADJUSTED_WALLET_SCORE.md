# Risk Adjusted Wallet Scores (The Anti-Degen Filter)

Naive win-rates fail cryptographically since the strategy naturally encourages actors holding `-90% losses` hoping for random recoveries scoring positive mathematical "wins" on paper.

By integrating `Risk Adjusted Scores` natively relying predominantly upon Drawdown variables combined against Sharpe and Sortino parameters, algorithmic behavior actively penalizes and decouples from "gamble" traces mapping cleanly against stable yield clusters.

## Mathematics
The module linearly drops all tracking confidence utilizing aggressive bounding if `MaxDrawdownPenalty > 0.6`, representing clusters failing routinely dropping accounts lower than `59%`.

**Missing Parameters Safety**
If Wallet Cohorts cannot supply historical records defining drawdown mapping points during evaluation windows, the metrics aggressively penalize the entity returning values representing `80% drawdowns`. This explicitly forces a `WalletSignalConfidence` equal to roughly 0 and sets off deep runtime blockers actively neutralizing following unknown entities entirely.
