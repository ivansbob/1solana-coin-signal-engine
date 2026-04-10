-- Dune Analytics Snapshot Query
-- Determines the percentage of Volume executing precisely on the block the LP was initialized.

WITH lp_init AS (
    SELECT mint, MIN(block_slot) as init_slot
    FROM solana.dex_trades
    GROUP BY mint
)
SELECT 
    t.mint,
    SUM(CASE WHEN t.block_slot = l.init_slot THEN t.amount_usd ELSE 0 END) as block_0_buy_volume,
    SUM(t.amount_usd) as total_buy_volume
FROM solana.dex_trades t
JOIN lp_init l ON t.mint = l.mint
GROUP BY t.mint;
