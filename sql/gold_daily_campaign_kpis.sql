-- Gold model: daily campaign KPI aggregates.
-- One row per campaign per day, rolled up across channels.
-- Executed by src/gold.py against the `silver_events` table.
SELECT
    event_date,
    campaign_id,
    campaign_name,
    SUM(impressions)                                   AS impressions,
    SUM(clicks)                                        AS clicks,
    ROUND(SUM(spend), 2)                               AS spend,
    SUM(conversions)                                   AS conversions,
    ROUND(SUM(revenue), 2)                             AS revenue,
    ROUND(100.0 * SUM(clicks) / NULLIF(SUM(impressions), 0), 2)
                                                       AS ctr_pct,
    ROUND(SUM(spend) / NULLIF(SUM(clicks), 0), 2)      AS cpc,
    ROUND(SUM(spend) / NULLIF(SUM(conversions), 0), 2) AS cost_per_conversion,
    ROUND(SUM(revenue) / NULLIF(SUM(spend), 0), 2)     AS roas,
    ROUND(100.0 * SUM(conversions) / NULLIF(SUM(clicks), 0), 2)
                                                       AS conversion_rate_pct
FROM silver_events
GROUP BY event_date, campaign_id, campaign_name
ORDER BY event_date, campaign_id;
