-- queries/dune/liquidity_refill_half_life.sql
-- Measures liquidity recovery after sharp dumps.

WITH trades AS (
    SELECT 
        date_trunc('minute', block_time) as minute,
        token_bought_address as token_address,
        SUM(amount_usd) as volume,
        APPROX_PERCENTILE((amount_usd / token_bought_amount), 0.5) as med_price
    FROM dex_solana.trades
    WHERE block_time > CURRENT_TIMESTAMP - INTERVAL '150' MINUTE
        AND token_bought_address = '{{token_address}}'
    GROUP BY 1, 2
),
liquidity_snapshots AS (
    -- Normally we'd use dex_solana.pools / liquidity depth, proxied here
    SELECT 
        minute,
        token_address,
        volume * med_price as approx_liquidity -- proxy mock
    FROM trades
),
peaks AS (
    SELECT 
        MAX(approx_liquidity) as liquidity_peak,
        MIN(minute) as peak_time
    FROM liquidity_snapshots
)

SELECT
    t.token_address,
    p.liquidity_peak,
    COALESCE(
        (SELECT approx_liquidity 
         FROM liquidity_snapshots s 
         WHERE s.minute >= p.peak_time + INTERVAL '2' MINUTE 
         ORDER BY s.minute 
         LIMIT 1), 
        0
    ) as liquidity_recovered
FROM (SELECT DISTINCT token_address FROM liquidity_snapshots) t
CROSS JOIN peaks p
