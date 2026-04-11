# Replay Truth and Price History Consolidation 

Reliable Replay data remains the most critical feature differentiating paper bots from functional on-chain systems.
By unifying how Price Truth evaluates timestamps, the algorithm fundamentally secures historical analysis.

## Replay Router Design
We query exact layers hierarchically verifying the most accurate representation of localized liquidity:
1. **Jupiter Quote:** Maximum realism reflecting executing swaps directly.
2. **Pyth Hermes:** Reliable Oracle tracing globally.
3. **GeckoTerminal:** Broad index mappings via OHLCV blocks.
4. **DexScreener:** Solid fallback proxy capturing micro-cap anomalies.

Tokens must execute across this precise sequence. Executions halting explicitly when standard windows map outside bounds ensure Replays operate identically comparable to live algorithms inherently suppressing hyper-optimistic traces blindly tracking garbage numbers.
