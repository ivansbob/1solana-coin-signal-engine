-- Wallet Lead-Lag Analysis: Temporal differences between smart wallet leaders and followers
-- Leaders: wallets with historical win-rate >= 65%
-- Computes average lag time between leader buys and subsequent follower buys within 60-minute window

WITH leader_wallets AS (
    SELECT wallet_address
    FROM wallet_win_rate_cache
    WHERE win_rate >= 0.65
),
recent_trades AS (
    SELECT
        token_address,
        trader,
        block_time,
        amount_usd
    FROM dex_solana.trades
    WHERE block_time >= NOW() - INTERVAL '60' MINUTE
      AND tx_type = 'buy'
),
leader_trades AS (
    SELECT * FROM recent_trades WHERE trader IN (SELECT wallet_address FROM leader_wallets)
),
follower_trades AS (
    SELECT * FROM recent_trades WHERE trader NOT IN (SELECT wallet_address FROM leader_wallets)
),
lead_lag_pairs AS (
    SELECT
        l.token_address,
        l.trader as leader_wallet,
        f.trader as follower_wallet,
        TIMESTAMPDIFF(SECOND, l.block_time, f.block_time) as lag_seconds,
        ROW_NUMBER() OVER (PARTITION BY f.token_address, f.trader ORDER BY f.block_time) as rn
    FROM leader_trades l
    JOIN follower_trades f ON l.token_address = f.token_address
    WHERE f.block_time > l.block_time
      AND TIMESTAMPDIFF(SECOND, l.block_time, f.block_time) BETWEEN 8 AND 180
      AND rn = 1  -- Take first follower trade after each leader trade
)

SELECT
    token_address,
    AVG(lag_seconds) as wallet_lead_lag_sec,
    COUNT(*) as lag_sample_count
FROM lead_lag_pairs
GROUP BY token_address
HAVING COUNT(*) >= 3  -- Require minimum samples</content>
<parameter name="filePath">queries/dune/wallet_lead_lag.sql