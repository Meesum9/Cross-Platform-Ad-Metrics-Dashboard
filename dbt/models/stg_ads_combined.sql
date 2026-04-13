-- Staging model for ads data
-- Cleans and standardizes the raw data

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
    FROM {{ source('analytics', 'ads_combined') }}
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
