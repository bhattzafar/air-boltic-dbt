/*
============================================================================
MODEL: int_success_pattern_market
-----------------------------------------------------------------------------
Grain: 1 row per destination_city

This model calculates a "success score" for each market (destination city) 
based on recent performance over the last 30 days.

Success score is a weighted combination of:
    • Seat utilization (efficiency)
    • Revenue generation (financial strength)
    • Repeat customers (retention)
    • Booking confirmation rate (reliability)

Each metric is normalized between 0 and 1 using min-max scaling.
This model enables ranking and segmentation of markets into tiers like
"High-performing", "At-risk", and "Emerging"

============================================================================
*/

WITH
base AS (
    SELECT
        destination_city
        , avg_utilization_pct_30d
        , total_revenue_30d
        , repeat_customer_pct_30d
        , pct_confirmed_orders_30d
    FROM
        {{ ref('int_market_success') }}
)

-- Min/max for normalization
, min_max AS (
    SELECT
        MIN(avg_utilization_pct_30d) AS min_utilization
        , MAX(avg_utilization_pct_30d) AS max_utilization

        , MIN(total_revenue_30d) AS min_revenue
        , MAX(total_revenue_30d) AS max_revenue

        , MIN(repeat_customer_pct_30d) AS min_repeat
        , MAX(repeat_customer_pct_30d) AS max_repeat

        , MIN(pct_confirmed_orders_30d) AS min_confirmed
        , MAX(pct_confirmed_orders_30d) AS max_confirmed
    FROM base
)

, scored AS (
    SELECT
        b.destination_city
        -- Normalized metrics
        , TRY_DIVIDE((b.avg_utilization_pct_30d - m.min_utilization), NULLIF(m.max_utilization - m.min_utilization, 0)) AS norm_utilization
        , TRY_DIVIDE((b.total_revenue_30d - m.min_revenue), NULLIF(m.max_revenue - m.min_revenue, 0)) AS norm_revenue
        , TRY_DIVIDE((b.repeat_customer_pct_30d - m.min_repeat), NULLIF(m.max_repeat - m.min_repeat, 0)) AS norm_repeat
        , TRY_DIVIDE((b.pct_confirmed_orders_30d - m.min_confirmed), NULLIF(m.max_confirmed - m.min_confirmed, 0)) AS norm_confirmed
    FROM
        base AS b
    CROSS JOIN
        min_max AS m
)

, final AS (
    SELECT
        destination_city
        , norm_utilization
        , norm_revenue
        , norm_repeat
        , norm_confirmed

        -- Weighted score
        , ROUND(
            0.3 * COALESCE(norm_utilization, 0) +
            0.3 * COALESCE(norm_revenue, 0) +
            0.2 * COALESCE(norm_repeat, 0) +
            0.2 * COALESCE(norm_confirmed, 0)
        , 3) AS success_score

        , CURRENT_TIMESTAMP AS last_updated_at
    FROM
        scored
)

SELECT * FROM final