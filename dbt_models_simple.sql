-- dbt Models for Facebook + Google Ads Marketing ROI Dashboard
-- These models calculate key performance metrics for each platform

-- Model 1: Base staging model
-- File: dbt/models/stg_ads_combined.sql

WITH source_data AS (
    SELECT 
        platform,
        campaign_id,
        ad_id,
        spend,
        impressions,
        clicks,
        conversions,
        date,
        -- Calculate metrics at row level
        CASE 
            WHEN clicks > 0 THEN spend / clicks 
            ELSE 0 
        END as cpc,
        CASE 
            WHEN impressions > 0 THEN (clicks * 100.0) / impressions 
            ELSE 0 
        END as ctr,
        CASE 
            WHEN conversions > 0 THEN spend / conversions 
            ELSE 0 
        END as cpa,
        CASE 
            WHEN clicks > 0 THEN (conversions * 100.0) / clicks 
            ELSE 0 
        END as conversion_rate
    FROM `analytics.ads_combined`
)

SELECT 
    platform,
    campaign_id,
    ad_id,
    spend,
    impressions,
    clicks,
    conversions,
    date,
    ROUND(cpc, 2) as cpc,
    ROUND(ctr, 2) as ctr,
    ROUND(cpa, 2) as cpa,
    ROUND(conversion_rate, 2) as conversion_rate
FROM source_data


-- Model 2: Daily performance by platform
-- File: dbt/models/fct_daily_performance.sql

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
    FROM `analytics.ads_combined`
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


-- Model 3: Platform comparison summary
-- File: dbt/models/fct_platform_comparison.sql

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
    FROM `analytics.ads_combined`
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


-- Model 4: Top performing campaigns
-- File: dbt/models/fct_top_campaigns.sql

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
    FROM `analytics.ads_combined`
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


-- Model 5: Weekly trends
-- File: dbt/models/fct_weekly_trends.sql

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
    FROM `analytics.ads_combined`
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
