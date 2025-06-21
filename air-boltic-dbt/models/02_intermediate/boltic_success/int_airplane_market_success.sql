{{
    config(
        materialized='table'
    )
}}

/*
============================================================================
MODEL: int_success_metrics__by_airline_market
-----------------------------------------------------------------------------
Grain: 1 row per (airplane_make, destination_city)

This model captures aircraft make-level performance across destinations,
over dynamic time windows (7d, 15d, 30d).

It enables strategic insights such as:
  • Identifying top-performing aircraft in each region
  • Supporting fleet procurement & route assignment
  • Advising airlines on model suitability for different markets

Metrics tracked include:
  • Trip volume and seat utilization
  • Booking trends and revenue
  • Ticket pricing, confirmations, and retention
============================================================================
*/

{% set windows = [7, 15, 30] %}

WITH

{% for days in windows %}

trip_metrics_{{ days }}d AS (
    SELECT
        dp.make AS airplane_make
        , t.destination_city
        , COUNT(DISTINCT t.trip_id) AS trip_count_{{ days }}d
        , AVG(t.utilization_pct) AS avg_utilization_pct_{{ days }}d
    FROM
        {{ ref('fact_trips') }} AS t
    LEFT JOIN
        {{ ref('dim_planes') }} AS dp
        ON
            t.airplane_id = dp.airplane_id
    WHERE
        t.start_timestamp >= DATEADD(day, -{{ days }}, CURRENT_DATE)
    GROUP BY
        1, 2
    {% if is_dev_env() %}
    LIMIT 100
    {% endif %}
)

, order_metrics_{{ days }}d AS (
    SELECT
        dp.make AS airplane_make
        , t.destination_city
        , COUNT(*) AS total_orders_{{ days }}d
        , SUM(o.price_eur) AS total_revenue_{{ days }}d
        , AVG(o.price_eur) AS avg_ticket_price_{{ days }}d
        , SUM(CASE WHEN o.status = 'confirmed' THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS pct_confirmed_orders_{{ days }}d
        , (COUNT(*) - COUNT(DISTINCT o.customer_id)) * 1.0 / COUNT(*) AS repeat_customer_pct_{{ days }}d
    FROM
        {{ ref('fact_orders') }} AS o
    LEFT JOIN
        {{ ref('fact_trips') }} AS t
        ON
            o.trip_id = t.trip_id
    LEFT JOIN
        {{ ref('dim_planes') }} AS dp
        ON
            t.airplane_id = dp.airplane_id
    WHERE
        t.start_timestamp >= DATEADD(day, -{{ days }}, CURRENT_DATE)
    GROUP BY
        1, 2
    {% if is_dev_env() %}
    LIMIT 100
    {% endif %}
)

{% if not loop.last %},{% endif %}

{% endfor %}

-- Final SELECT
SELECT
    tm7.airplane_make
    , tm7.destination_city

    {% for days in windows %}
    -- {{ days }}d window metrics
    , tm{{ days }}.trip_count_{{ days }}d
    , tm{{ days }}.avg_utilization_pct_{{ days }}d
    , om{{ days }}.total_orders_{{ days }}d
    , om{{ days }}.total_revenue_{{ days }}d
    , om{{ days }}.avg_ticket_price_{{ days }}d
    , om{{ days }}.pct_confirmed_orders_{{ days }}d
    , om{{ days }}.repeat_customer_pct_{{ days }}d
    {% endfor %}

FROM
    trip_metrics_7d AS tm7

{% for days in windows %}
LEFT JOIN
    order_metrics_{{ days }}d AS om{{ days }}
    ON
        tm7.airplane_make = om{{ days }}.airplane_make
        AND tm7.destination_city = om{{ days }}.destination_city
{% if days != 7 %}
LEFT JOIN
    trip_metrics_{{ days }}d AS tm{{ days }}
    ON
        tm7.airplane_make = tm{{ days }}.airplane_make
        AND tm7.destination_city = tm{{ days }}.destination_city
{% endif %}
{% endfor %}