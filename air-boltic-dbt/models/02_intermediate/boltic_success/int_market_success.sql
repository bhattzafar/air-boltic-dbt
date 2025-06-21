{{
    config(
        materialized='table'
    )
}}

/*
============================================================================
MODEL: int_success_metrics__by_market
-----------------------------------------------------------------------------
Grain: 1 row per destination_city

This model calculates Air Boltic's market-level performance metrics across
three rolling time windows: 7, 15, and 30 days.

A "market" is defined by destination_city — the destination endpoint of trips.

For each window, this model computes:
    Trip volume and average seat utilization
    Total revenue and ticket pricing
    Share of confirmed bookings
    Repeat customer rate

These time-aware KPIs allow for tracking seasonal trends, regional momentum,
and expansion potential, forming the basis for market success scoring and
downstream alerting.

NOTE:
All the trips in fact_trips are from 2024 thus there will be no output of this model.
The model adds some static data from seed file for output visualization. (may subject to data visualization/ number format errors)
============================================================================
*/

{% set windows = [7, 15, 30] %}

-- Trip metrics CTEs
{% for days in windows %}
{% if loop.first %}
WITH trip_metrics_{{ days }}d AS {{ air_boltic_trip_metrics_window(days) }}
{% else %}
, trip_metrics_{{ days }}d AS {{ air_boltic_trip_metrics_window(days) }}
{% endif %}
{% endfor %}

-- Generate order metrics per window
{% for days in windows %}
, order_metrics_{{ days }}d AS
{{ air_boltic_order_metrics_window(days, 'destination_city') }}
{% endfor %}

-- Final SELECT
, final AS (
    SELECT
        tm7.destination_city
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
            tm7.destination_city = om{{ days }}.destination_city
    {% if days != 7 %}
    LEFT JOIN
        trip_metrics_{{ days }}d AS tm{{ days }}
        ON
            tm7.destination_city = tm{{ days }}.destination_city
    {% endif %}
    {% endfor %}
)

-- All the trips in fact_trips are from 2024 thus there will be no output of this model.
-- The following cte adds some static data from seed file for output visualization 
, union_sample_data AS (
    SELECT * FROM final
    UNION ALL
    SELECT * FROM {{ ref('int_market_success_sample_result') }}
)

SELECT * FROM union_sample_data