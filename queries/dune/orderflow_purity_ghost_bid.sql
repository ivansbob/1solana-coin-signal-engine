-- Orderflow Purity and Ghost Bid Score Query
-- Computes ghost bid ratio, wash trade proxy, and organic buy ratio
-- Uses dex_solana.trades and Helius transaction parsing

WITH token_trades AS (
    SELECT
        token_address,
        trader,
        amount_usd,
        block_time,
        tx_hash,
        CASE WHEN amount > 0 THEN 'buy' ELSE 'sell' END as trade_type
    FROM dex_solana.trades
    WHERE token_address = '{{token_address}}'
    AND block_time >= NOW() - INTERVAL '{{window_minutes}} minutes'
),

-- Ghost bids: failed transactions or bids that didn't fill
ghost_bids AS (
    SELECT
        COUNT(*) as ghost_bid_count
    FROM helius.transactions_parsed hp
    WHERE hp.token_address = '{{token_address}}'
    AND hp.block_time >= NOW() - INTERVAL '{{window_minutes}} minutes'
    AND (
        hp.status = 'failed'  -- Failed transactions
        OR (hp.instruction_type = 'place_order' AND hp.fill_amount = 0)  -- Unfilled orders
    )
),

-- Wash trade detection: repeated wallet pairs trading back and forth
wash_trades AS (
    SELECT
        SUM(amount_usd) as wash_volume
    FROM (
        SELECT
            t1.trader as wallet_a,
            t2.trader as wallet_b,
            SUM(t1.amount_usd) as volume
        FROM token_trades t1
        JOIN token_trades t2 ON t1.tx_hash != t2.tx_hash
            AND t1.trader < t2.trader  -- Avoid duplicates
            AND ABS(EXTRACT(EPOCH FROM (t1.block_time - t2.block_time))) < 300  -- Within 5 minutes
        WHERE t1.trade_type != t2.trade_type  -- Buy/sell pair
        GROUP BY t1.trader, t2.trader
        HAVING COUNT(*) > 2  -- Multiple round trips
    ) repeated_pairs
),

-- Organic buyers: unique wallets with genuine trading patterns
organic_buyers AS (
    SELECT
        COUNT(DISTINCT trader) as unique_organic_buyers
    FROM token_trades
    WHERE trade_type = 'buy'
    AND trader NOT IN (
        -- Exclude wallets involved in wash trades
        SELECT DISTINCT trader FROM wash_trades
    )
    AND trader NOT IN (
        -- Exclude ghost bid wallets
        SELECT DISTINCT trader FROM ghost_bids
    )
),

total_stats AS (
    SELECT
        COUNT(*) as total_tx,
        COUNT(DISTINCT CASE WHEN trade_type = 'buy' THEN trader END) as total_buyers,
        SUM(amount_usd) as total_volume
    FROM token_trades
)

SELECT
    COALESCE(g.ghost_bid_count, 0) / NULLIF(ts.total_tx, 0) as ghost_bid_ratio,
    COALESCE(w.wash_volume, 0) / NULLIF(ts.total_volume, 0) as wash_trade_proxy,
    COALESCE(o.unique_organic_buyers, 0) / NULLIF(ts.total_buyers, 0) as organic_buy_ratio
FROM total_stats ts
CROSS JOIN ghost_bids g
CROSS JOIN wash_trades w
CROSS JOIN organic_buyers o;