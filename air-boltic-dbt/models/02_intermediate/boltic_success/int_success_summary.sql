/*
============================================================================
MODEL: fact_success_summary
-----------------------------------------------------------------------------
Grain: 1 row per snapshot date (event-level metric rollups)

This model provides a longitudinal summary of Air Boltic's overall business
performance across different metrics (revenue, trip volume, utilization, etc.),
calculated on a daily rolling basis.

Unlike static metrics based on CURRENT_DATE, this design introduces a dynamic
date spine to simulate daily snapshots — allowing us to observe performance
across all historical events (trips/orders).

============================================================================
*/

{{ config(
    materialized = 'incremental',
    unique_key = ['snapshot_date'],
    on_schema_change = 'sync_all_columns'
) }}

-- Import useful macros
{% set start_date = '2023-01-01' %}
{% set end_date = '2025-12-31' %} -- Horizon for future planning

-- Date spine generation
WITH
date_spine AS (
    {{ dbt_utils.date_spine(0
        datepart="day",
        start_date=start_date,
        end_date=end_date
    ) }}
)

-- Join fact_trips and fact_orders onto the spine
, trips_extended AS (
    SELECT
        ds.date_day AS snapshot_date
        , t.trip_id
        , t.utilization_pct
        , t.start_timestamp
    FROM
        date_spine ds
    LEFT JOIN
        {{ ref('fact_trips') }} AS t
        ON
            DATE(t.start_timestamp) = ds.date_day
)

, orders_extended AS (
    SELECT
        ds.date_day AS snapshot_date
        , o.order_id
        , o.price_eur
        , o.status
        , o.customer_id
        , t.start_timestamp
    FROM
        date_spine AS ds
    LEFT JOIN
        {{ ref('fact_orders') }} AS o
        ON
            DATE(o.start_timestamp) = ds.date_day
    LEFT JOIN
        {{ ref('fact_trips') }} AS t
        ON
            o.trip_id = t.trip_id
)

-- Aggregate business metrics per snapshot day
, aggregated_trips AS (
    SELECT
        snapshot_date
        , COUNT(DISTINCT trip_id) AS trip_count
        , AVG(utilization_pct) AS avg_utilization_pct
    FROM
        trips_extended
    GROUP BY 
        snapshot_date
)

, aggregated_orders AS (
    SELECT
        snapshot_date
        , COUNT(*) AS total_orders
        , SUM(price_eur) AS total_revenue
        , AVG(price_eur) AS avg_ticket_price
        , SUM(CASE WHEN status = 'confirmed' THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS pct_confirmed_orders
        , (COUNT(*) - COUNT(DISTINCT customer_id)) * 1.0 / COUNT(*) AS repeat_customer_pct
    FROM
        orders_extended
    GROUP BY
        snapshot_date
)

-- Final SELECT
SELECT
    trips.snapshot_date
    , trips.trip_count
    , trips.avg_utilization_pct
    , orders.total_orders
    , orders.total_revenue
    , orders.avg_ticket_price
    , orders.pct_confirmed_orders
    , orders.repeat_customer_pct
FROM aggregated_trips trips
LEFT JOIN aggregated_orders orders
    ON trips.snapshot_date = orders.snapshot_date