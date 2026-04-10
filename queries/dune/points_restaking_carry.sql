-- Points / Restaking Carry Score
-- Computes velocity of points accrual and blended APY adjusted for inflation
-- Sources: Dune (points data), DefiLlama API (TVL, APY, emissions)

WITH points_data AS (
    SELECT
        token_address,
        SUM(CASE WHEN timestamp >= NOW() - INTERVAL '7 days' THEN points_amount ELSE 0 END) as points_accrued_7d,
        SUM(CASE WHEN timestamp >= NOW() - INTERVAL '30 days' THEN points_amount ELSE 0 END) as points_accrued_30d
    FROM dune.points_dataset
    WHERE token_address = :token_address
    GROUP BY token_address
),

yield_data AS (
    SELECT
        p.token_address,
        COALESCE(dl.blended_apy, 0.0) as blended_apy,
        COALESCE(dl.token_inflation_rate, 0.0) as token_inflation_rate
    FROM points_data p
    LEFT JOIN defillama.token_metrics dl ON p.token_address = dl.token_address
    WHERE dl.timestamp >= NOW() - INTERVAL '1 day'
    ORDER BY dl.timestamp DESC
    LIMIT 1
)

SELECT
    p.token_address,
    -- PointsVelocity = points_accrued_7d / (points_accrued_30d + 1)
    CASE 
        WHEN p.points_accrued_30d IS NOT NULL AND p.points_accrued_30d >= 0 
        THEN p.points_accrued_7d / (p.points_accrued_30d + 1.0)
        ELSE NULL
    END as points_velocity,
    
    -- RestakingYieldProxy = blended_apy * (1 - token_inflation_rate)
    CASE 
        WHEN y.blended_apy IS NOT NULL AND y.token_inflation_rate IS NOT NULL
        THEN y.blended_apy * (1.0 - y.token_inflation_rate)
        ELSE NULL
    END as restaking_yield_proxy,
    
    -- Provenance data
    p.points_accrued_7d,
    p.points_accrued_30d,
    y.blended_apy,
    y.token_inflation_rate,
    NOW() as calculation_timestamp
FROM points_data p
LEFT JOIN yield_data y ON p.token_address = y.token_address
WHERE p.token_address = :token_address;