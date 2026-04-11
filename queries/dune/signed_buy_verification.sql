-- Dune Analytics Snapshot Query
-- Validates organic signatures verifying volume across Solana

SELECT 
    mint, 
    SUM(amount_usd) as signed_buy_volume 
FROM solana.dex_trades 
WHERE 
    block_date >= CURRENT_DATE - INTERVAL '1' DAY
    AND maker_is_signer = TRUE
    AND tx_signer NOT IN (SELECT address FROM intermediary_known_bots)
GROUP BY mint;
