-- Platform comparison summary
-- Aggregates all data by platform for high-level comparison

WITH platform_totals AS (
    SELECT 
        platform,
        SUM(spend) as total_spend,
        SUM(impressions) as total_impressions,
        SUM(clicks) as total_clicks,
        SUM(conversions) as total_conversions,
        COUNT(DISTINCT date) as active_days,
        CASE 
            WHEN SUM(clicks) > 0 THEN SUM(spend) / SUM(clicks) 
            ELSE 0 
        END as avg_cpc,
        CASE 
            WHEN SUM(impressions) > 0 THEN (SUM(clicks) * 100.0) / SUM(impressions) 
            ELSE 0 
        END as avg_ctr,
        CASE 
            WHEN SUM(conversions) > 0 THEN SUM(spend) / SUM(conversions) 
            ELSE 0 
        END as avg_cpa,
        CASE 
            WHEN SUM(clicks) > 0 THEN (SUM(conversions) * 100.0) / SUM(clicks) 
            ELSE 0 
        END as avg_conversion_rate
    FROM {{ ref('stg_ads_combined') }}
    GROUP BY platform
)

SELECT 
    platform,
    ROUND(total_spend, 2) as total_spend,
    total_impressions,
    total_clicks,
    total_conversions,
    active_days,
    ROUND(avg_cpc, 2) as avg_cpc,
    ROUND(avg_ctr, 2) as avg_ctr,
    ROUND(avg_cpa, 2) as avg_cpa,
    ROUND(avg_conversion_rate, 2) as avg_conversion_rate,
    -- Calculate efficiency metrics
    ROUND(avg_cpc / NULLIF(avg_ctr, 0), 2) as cpc_to_ctr_ratio,
    ROUND(total_conversions / NULLIF(total_spend, 0) * 1000, 2) as conversions_per_1k_spend
FROM platform_totals
ORDER BY platform
