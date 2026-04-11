-- queries/dune/vol_compression_breakout.sql
-- Computes ATR compression over 5m / 60m to identify impending strong moves.

WITH minutes AS (
    SELECT 
        date_trunc('minute', block_time) as minute,
        token_bought_address as token_address,
        MAX(amount_usd / token_bought_amount) as high_price,
        MIN(amount_usd / token_bought_amount) as low_price,
        -- Opening and closing proxied via array agg typically, simplified here
        APPROX_PERCENTILE((amount_usd / token_bought_amount), 0.5) as med_price
    FROM dex_solana.trades
    WHERE block_time > CURRENT_TIMESTAMP - INTERVAL '60' MINUTE
        AND token_bought_address = '{{token_address}}'
    GROUP BY 1, 2
),
atr_raw AS (
    SELECT
        minute,
        token_address,
        (high_price - low_price) as true_range
    FROM minutes
),
aggs AS (
    SELECT 
        token_address,
        AVG(CASE WHEN minute > CURRENT_TIMESTAMP - INTERVAL '5' MINUTE THEN true_range ELSE NULL END) as atr_5m,
        AVG(true_range) as atr_60m,
        MAX(CASE WHEN minute = (SELECT MAX(minute) FROM minutes) THEN high_price ELSE NULL END) as last_high,
        MIN(CASE WHEN minute = (SELECT MIN(minute) FROM minutes) THEN low_price ELSE NULL END) as first_low
    FROM atr_raw
    GROUP BY 1
)

SELECT 
    token_address,
    atr_5m,
    atr_60m,
    ((last_high - first_low) / first_low) * 100 as price_change_15m_pct
FROM aggs
