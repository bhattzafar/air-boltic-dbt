{{ config(
    materialized='table'
) }}

{% set windows = [7, 15, 30] %}

-- Core Orders + Segment Attributes
WITH
base_orders AS (
    SELECT
        customer_id
        , order_id
        , booking_ts
        , price_eur
        , status
        , customer_group_id
        , customer_group_name
        , customer_country
        , origin_city
        , destination_city
        , airplane_manufacturer
        , plane_size_segment
        , plane_distance_segment

        -- 👇 Primary key per segment (for joins)
        , {{ dbt_utils.generate_surrogate_key([
            'customer_group_id',
            'customer_group_name',
            'airplane_manufacturer',
            'plane_size_segment',
            'plane_distance_segment'
        ]) }} AS segment_key
    FROM
        {{ ref("fact_orders") }}
)

-- 👇 Future enrichments to customer segmentation
/*
, customer_dimensions AS (
    SELECT
        customer_id
        , customer_signup_period -- (e.g., '3_months', '1_day', etc.)
        , customer_intake_funnel -- (e.g., 'organic', 'paid_social')
        , customer_payment_plan -- (e.g., 'free', 'premium', 'enterprise')
        , customer_country_enriched -- (canonical country assignment)
        , customer_campaign_code -- (discount/campaign tags)
        ...
    FROM {{ ref('dim_customers') }}
)
*/

-- Windowed segment aggregations
{% for days in windows %}
, segment_metrics_{{ days }}d AS (
    SELECT
        segment_key
        , customer_group_id
        , customer_group_name
        , airplane_manufacturer
        , plane_size_segment
        , plane_distance_segment

        -- Metrics
        , COUNT(DISTINCT order_id) AS total_orders_{{ days }}d
        , COUNT(DISTINCT customer_id) AS unique_customers_{{ days }}d
        , SUM(price_eur) AS total_revenue_{{ days }}d
        , AVG(price_eur) AS avg_ticket_price_{{ days }}d
        , SUM(CASE WHEN LOWER(status) = 'cancelled' THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS cancellation_rate_{{ days }}d
    FROM
        base_orders
    -- ADD JOIN TO DIM_CUSTOMERS TO GET OTHER DIMENSIONS AS WELL
    WHERE
        booking_ts >= DATEADD(day, -{{ days }}, CURRENT_DATE)
    GROUP BY
        1, 2, 3, 4, 5, 6
)
{% endfor %}

, final AS (
    -- Final select with JOINs by surrogate key
    SELECT
        seg7.segment_key
        , seg7.customer_group_id
        , seg7.customer_group_name
        , seg7.airplane_manufacturer
        , seg7.plane_size_segment
        , seg7.plane_distance_segment

        {% for days in windows %}
        , seg{{ days }}.total_orders_{{ days }}d
        , seg{{ days }}.unique_customers_{{ days }}d
        , seg{{ days }}.total_revenue_{{ days }}d
        , seg{{ days }}.avg_ticket_price_{{ days }}d
        , seg{{ days }}.cancellation_rate_{{ days }}d
        {% endfor %}

    FROM
        segment_metrics_7d AS seg7
    {% for days in windows if days != 7 %}
    LEFT JOIN
        segment_metrics_{{ days }}d AS seg{{ days }}
        ON
            seg7.segment_key = seg{{ days }}.segment_key
    {% endfor %}
)

, union_with_sample_data AS (
    SELECT * FROM final
    UNION ALL
    SELECT * FROM {{ ref('int_customer_segment_analysis_sample') }}
)

SELECT * FROM union_with_sample_data
