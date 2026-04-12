-- Dynamic Wallet Safety Checks extracting structural bounds resolving metrics
-- Dune explicitly links this to public clustering outputs preventing dependencies mapping closed analytics platforms

WITH clustered_wallets AS (
    SELECT 
        wallet_address,
        cohort_id
    FROM wallet_cohorts
    WHERE status = 'ACTIVE'
),
wallet_metrics_90d AS (
    SELECT 
        c.wallet_address,
        c.cohort_id,
        CASE WHEN stddev(t.daily_pnl) > 0 THEN avg(t.daily_pnl) / stddev(t.daily_pnl) ELSE 0 END AS sharpe_90d,
        -- Sortino Proxy ignores upside standard deviation
        CASE WHEN stddev(CASE WHEN t.daily_pnl < 0 THEN t.daily_pnl ELSE NULL END) > 0 
             THEN avg(t.daily_pnl) / stddev(CASE WHEN t.daily_pnl < 0 THEN t.daily_pnl ELSE NULL END) 
             ELSE 0 END AS sortino_90d,
        -- Profit Factor
        SUM(CASE WHEN t.daily_pnl > 0 THEN t.daily_pnl ELSE 0 END) / 
        ABS(SUM(CASE WHEN t.daily_pnl < 0 THEN t.daily_pnl ELSE 0 END)) AS profit_factor,
        -- Drawdown math is structurally handled sequentially in memory normally, using proxies here tracking local equity drops
        MAX(t.equity_peak) - MIN(t.equity_trough) / MAX(t.equity_peak) AS max_drawdown_90d
    FROM clustered_wallets c
    LEFT JOIN wallet_pnl_stream_90d t ON c.wallet_address = t.wallet_address
    GROUP BY c.wallet_address, c.cohort_id
)

SELECT 
    cohort_id,
    AVG(sharpe_90d) as avg_sharpe_90d,
    AVG(sortino_90d) as avg_sortino_90d,
    AVG(profit_factor) as avg_profit_factor,
    AVG(max_drawdown_90d) as avg_max_drawdown_90d,
    COUNT(wallet_address) as cohort_size
FROM wallet_metrics_90d
GROUP BY cohort_id
