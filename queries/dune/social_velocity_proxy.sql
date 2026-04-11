-- Dune SQL Proxy defining mapping requirements pulling historical social velocities alongside execution volume
-- This is a baseline proxy intended to operate on DexScreener hooks or free Twitter APIs

WITH social_mentions AS (
    SELECT
        token_address,
        COUNT(*) AS total_mentions,
        SUM(CASE WHEN created_at >= NOW() - INTERVAL '10' MINUTE THEN 1 ELSE 0 END) AS mentions_10m,
        SUM(CASE WHEN created_at >= NOW() - INTERVAL '60' MINUTE THEN 1 ELSE 0 END) AS mentions_60m,
        COUNT(DISTINCT author_id) AS unique_aggregators
    FROM raw_social_streams
    WHERE created_at >= NOW() - INTERVAL '1' HOUR
    GROUP BY token_address
),
bot_activity_proxy AS (
    -- Simplest indicator of bot presence: mass repetition of identical texts with identical timestamps
    SELECT 
        token_address,
        COUNT(*) as copy_paste_hits
    FROM raw_social_streams
    WHERE created_at >= NOW() - INTERVAL '1' HOUR
    GROUP BY text_hash, token_address
    HAVING COUNT(*) > 10
)

SELECT 
    s.token_address,
    s.mentions_10m as social_velocity_10m,
    s.mentions_60m as social_velocity_60m,
    s.unique_aggregators,
    COALESCE(b.copy_paste_hits, 0) as bot_proxy_hits
FROM social_mentions s
LEFT JOIN bot_activity_proxy b ON s.token_address = b.token_address
ORDER BY s.mentions_10m DESC
LIMIT 100;
