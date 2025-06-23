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


WITH

date_spine AS (
    SELECT
        date_day::DATE
    FROM
        {{ ref('stg_dates') }}
    WHERE
        {% if is_incremental() %}
            date_day >= (SELECT MAX(snapshot_date) FROM {{ this }})
        {% else %}
            date_day >= '2023-01-01'
        {% endif %}
)

-- Join fact_trips and fact_orders onto the spine
, trips_extended AS (
    SELECT
        ds.date_day AS snapshot_date
        , t.trip_id
        , t.utilization_pct
        , t.start_timestamp
    FROM
        date_spine AS ds
    LEFT JOIN
        {{ ref('fact_trips') }} AS t
        ON
            DATE(t.start_timestamp) = ds.date_day
)

, orders_extended AS (
    SELECT
        ds.snapshot_date
        , ds.trip_id
        , o.order_id
        , o.price_eur
        , o.status
        , o.customer_id
        , ds.start_timestamp
        , ds.utilization_pct
    FROM
        trips_extended AS ds
    LEFT JOIN
        {{ ref('fact_orders') }} AS o
        ON
            o.trip_id = ds.trip_id
)

-- Aggregate business metrics per snapshot day
, aggregated_trips AS (
    SELECT
        snapshot_date
        , ZEROIFNULL(COUNT(DISTINCT trip_id)) AS trip_count
        , ZEROIFNULL(COUNT(DISTINCT order_id)) AS order_count
        , ZEROIFNULL(SUM(price_eur)) AS total_revenue
        , ZEROIFNULL(AVG(price_eur)) AS avg_ticket_price
        , ZEROIFNULL(AVG(utilization_pct)) AS avg_utilization_pct
        , ZEROIFNULL(TRY_DIVIDE(SUM(CASE WHEN LOWER(status) = 'finished' THEN 1 ELSE 0 END) * 1.0, COUNT(DISTINCT order_id))::INT) AS pct_confirmed_orders
        , ZEROIFNULL(COUNT(DISTINCT customer_id)::INT) AS unique_customers
        , ZEROIFNULL(TRY_DIVIDE((COUNT(DISTINCT order_id) - COUNT(DISTINCT customer_id)) * 1.0, NULLIF(COUNT(DISTINCT order_id), 0))::INT) AS repeat_customer_count
    FROM
        orders_extended
    GROUP BY
        snapshot_date
)

SELECT
    snapshot_date
    , trip_count
    , order_count
    , total_revenue
    , avg_ticket_price
    , avg_utilization_pct
    , pct_confirmed_orders
    , unique_customers
    , repeat_customer_count
    , CURRENT_TIMESTAMP AS last_updated_at
FROM
    aggregated_trips
{% if is_dev_env() %}
WHERE
    DATE_TRUNC('DD', snapshot_date)::DATE >= DATE_ADD(CURRENT_DATE(), -90);
{% endif %}
