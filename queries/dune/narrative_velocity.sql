-- Dune SQL for Narrative Velocity Proxy: X + Telegram mentions acceleration
-- Collects social mentions specifically from X (Twitter) and Telegram platforms

WITH narrative_mentions AS (
    SELECT
        token_address,
        COUNT(*) AS total_mentions,
        SUM(CASE WHEN created_at >= NOW() - INTERVAL '5' MINUTE THEN 1 ELSE 0 END) AS mentions_5m,
        SUM(CASE WHEN created_at >= NOW() - INTERVAL '60' MINUTE THEN 1 ELSE 0 END) AS mentions_60m,
        COUNT(DISTINCT author_id) AS unique_authors,
        platform  -- 'x' or 'telegram'
    FROM raw_social_streams
    WHERE platform IN ('x', 'telegram')
        AND created_at >= NOW() - INTERVAL '1' HOUR
    GROUP BY token_address, platform
),

aggregated_mentions AS (
    SELECT
        token_address,
        SUM(mentions_5m) AS narrative_velocity_5m,
        SUM(mentions_60m) AS narrative_velocity_60m,
        SUM(total_mentions) AS total_mentions_combined,
        SUM(unique_authors) AS unique_authors_combined
    FROM narrative_mentions
    GROUP BY token_address
)

SELECT
    token_address,
    narrative_velocity_5m,
    narrative_velocity_60m,
    total_mentions_combined,
    unique_authors_combined,
    -- Acceleration ratio for quick reference
    CASE
        WHEN narrative_velocity_60m > 0 THEN narrative_velocity_5m / narrative_velocity_60m
        ELSE NULL
    END AS raw_acceleration_ratio
FROM aggregated_mentions
ORDER BY narrative_velocity_5m DESC
LIMIT 100;