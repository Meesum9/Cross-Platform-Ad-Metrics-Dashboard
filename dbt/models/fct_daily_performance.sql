-- Daily performance metrics by platform
-- Aggregates data to daily level for trend analysis

WITH daily_metrics AS (
    SELECT 
        platform,
        date,
        SUM(spend) as total_spend,
        SUM(impressions) as total_impressions,
        SUM(clicks) as total_clicks,
        SUM(conversions) as total_conversions,
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
    GROUP BY platform, date
)

SELECT 
    platform,
    date,
    ROUND(total_spend, 2) as total_spend,
    total_impressions,
    total_clicks,
    total_conversions,
    ROUND(avg_cpc, 2) as avg_cpc,
    ROUND(avg_ctr, 2) as avg_ctr,
    ROUND(avg_cpa, 2) as avg_cpa,
    ROUND(avg_conversion_rate, 2) as avg_conversion_rate
FROM daily_metrics
ORDER BY date, platform
