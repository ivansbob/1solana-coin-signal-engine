# Orderflow Purity & Sybil Shield

Solana memecoins frequently manipulate metrics (volume, holders, cluster buys) to bait entry logic. The `Orderflow Purity Score` is developed as a deterministic shield protecting `TotalScore` from manipulated metrics.

## Overview

The shield revolves around defining **Quality over Quantity**. By querying and validating exact transaction payloads on chain:
- **Signed Buy Vol:** Evaluates how much of the buying volume comes strictly via user interaction vs algorithmic routing manipulation.
- **Block-0 Snipes:** Identifies dev team pre-loads capturing initial liquidity. High concentrations usually result in dumps.
- **Repeat Buyers:** Detects immediate wash trading to simulate momentum.
- **Sybil Clustering:** Aggregates related deterministic wallet clusters to check if volumes come directly from coordinated fake organic behavior.

## Integration

The metric `orderflow_purity_score` operates from 0.0 -> 1.0 returning rigorous calculations tracking manipulation indices. 

1. **Scoring Factor:** TotalScore computes dynamically augmenting the base algorithmic momentum directly with `0.13 * purity_score` allowing high quality flow significantly broader coverage.
2. **Hard Blockers:** If manipulating events pass a hard threshold:
    - Purity < 0.45
    - Block 0 Sniping > 35% of Total
    These are instantly rejected blocking execution downstream.
3. **Honest Degrade Defaults:** If token context omits block 0 values or signature verifications, the formulas defensively default to partial penalty configurations. This secures the logic natively ensuring missing data isn't evaluated as purely organic.

## Operations
Dune Analytics routines query historical patterns and integrate offline. Check `./queries/dune` for sample SQL templates validating `signer origins` and `slot block-0 execution metrics`.
