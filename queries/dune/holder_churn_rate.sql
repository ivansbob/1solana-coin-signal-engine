-- queries/dune/holder_churn_rate.sql
-- Computes internal Holder Churn by identifying new buyers within 24h vs total unique buyers.

WITH trades_24h AS (
    SELECT 
        trader_id as buyer_address
    FROM dex_solana.trades
    WHERE block_time > CURRENT_TIMESTAMP - INTERVAL '24' HOUR
        AND token_bought_address = '{{token_address}}'
),
trades_all_time AS (
    SELECT 
        trader_id as buyer_address
    FROM dex_solana.trades
    WHERE block_time <= CURRENT_TIMESTAMP - INTERVAL '24' HOUR
        AND token_bought_address = '{{token_address}}'
),
unique_24h AS (
    SELECT DISTINCT buyer_address FROM trades_24h
),
unique_past AS (
    SELECT DISTINCT buyer_address FROM trades_all_time
),
buyer_stats AS (
    SELECT
        COUNT(u.buyer_address) as total_buyers_24h,
        COUNT(CASE WHEN p.buyer_address IS NULL THEN 1 END) as new_buyers_24h
    FROM unique_24h u
    LEFT JOIN unique_past p ON u.buyer_address = p.buyer_address
)

SELECT
    total_buyers_24h,
    new_buyers_24h,
    CAST(new_buyers_24h AS DOUBLE) / NULLIF(total_buyers_24h, 0) as holder_churn_rate_24h,
    1.0 - (CAST(new_buyers_24h AS DOUBLE) / NULLIF(total_buyers_24h, 0)) as returning_buyers_ratio_24h
FROM buyer_stats
