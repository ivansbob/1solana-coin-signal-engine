-- Cumulative Delta Divergence Query for Dune Analytics
-- Calculates cumulative buy/sell volume divergence vs price movement over 24h
-- Only uses verified on-chain trades

WITH token_trades AS (
    SELECT
        token_address,
        CASE
            WHEN side = 'buy' THEN amount_usd
            ELSE 0
        END AS buy_volume_usd,
        CASE
            WHEN side = 'sell' THEN amount_usd
            ELSE 0
        END AS sell_volume_usd,
        price_usd,
        block_time
    FROM dex_solana.trades
    WHERE token_address = '{{token_address}}'
    AND block_time >= NOW() - INTERVAL '24 hours'
    AND verified = true  -- Only verified on-chain trades
),

volume_summary AS (
    SELECT
        token_address,
        SUM(buy_volume_usd) AS cum_buy_volume_24h,
        SUM(sell_volume_usd) AS cum_sell_volume_24h
    FROM token_trades
    GROUP BY token_address
),

price_change AS (
    SELECT
        token_address,
        (LAST_VALUE(price_usd) OVER (ORDER BY block_time) -
         FIRST_VALUE(price_usd) OVER (ORDER BY block_time)) /
        NULLIF(FIRST_VALUE(price_usd) OVER (ORDER BY block_time), 0) * 10000 AS price_change_bps_24h
    FROM token_trades
    QUALIFY ROW_NUMBER() OVER (ORDER BY block_time DESC) = 1
)

SELECT
    v.token_address,
    v.cum_buy_volume_24h,
    v.cum_sell_volume_24h,
    p.price_change_bps_24h,
    (v.cum_buy_volume_24h - v.cum_sell_volume_24h) /
    NULLIF(p.price_change_bps_24h + 1, 0) AS cum_delta_divergence
FROM volume_summary v
LEFT JOIN price_change p ON v.token_address = p.token_address