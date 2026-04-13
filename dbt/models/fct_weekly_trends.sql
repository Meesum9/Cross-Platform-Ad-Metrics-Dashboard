-- Weekly trends
-- Aggregates data by week for trend analysis

WITH weekly_metrics AS (
    SELECT 
        platform,
        EXTRACT(WEEK FROM date) as week_number,
        EXTRACT(YEAR FROM date) as year,
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
        END as avg_cpa
    FROM {{ ref('stg_ads_combined') }}
    GROUP BY platform, EXTRACT(WEEK FROM date), EXTRACT(YEAR FROM date)
)

SELECT 
    platform,
    year,
    week_number,
    ROUND(total_spend, 2) as total_spend,
    total_impressions,
    total_clicks,
    total_conversions,
    ROUND(avg_cpc, 2) as avg_cpc,
    ROUND(avg_ctr, 2) as avg_ctr,
    ROUND(avg_cpa, 2) as avg_cpa
FROM weekly_metrics
ORDER BY year, week_number, platform
