-- Top performing campaigns
-- Identifies best performing campaigns by conversions

WITH campaign_metrics AS (
    SELECT 
        platform,
        campaign_id,
        SUM(spend) as total_spend,
        SUM(impressions) as total_impressions,
        SUM(clicks) as total_clicks,
        SUM(conversions) as total_conversions,
        COUNT(DISTINCT date) as days_active,
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
    GROUP BY platform, campaign_id
)

SELECT 
    platform,
    campaign_id,
    ROUND(total_spend, 2) as total_spend,
    total_impressions,
    total_clicks,
    total_conversions,
    days_active,
    ROUND(avg_cpc, 2) as avg_cpc,
    ROUND(avg_ctr, 2) as avg_ctr,
    ROUND(avg_cpa, 2) as avg_cpa,
    ROUND(total_conversions / NULLIF(total_spend, 0) * 1000, 2) as conversions_per_1k_spend
FROM campaign_metrics
ORDER BY total_conversions DESC
LIMIT 20
