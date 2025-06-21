{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='trip_id',
        on_schema_change='sync_all_columns',
        cluster_by=['airplane_id']
    )
}}

/*
================================================================================
MODEL: fact_trips
--------------------------------------------------------------------------------
Grain: 1 row per trip_id

This model aggregates trip-level data to provide insights into operational
efficiency and network performance. It is designed to answer questions like:

- How full are our planes (seat utilization)?
- How much revenue does each trip generate?
- What is the average trip duration by route or aircraft?
- Which trips/routes are performing best by seat fill or revenue?

Key metrics include:
- total_orders
- seats_booked
- trip_revenue
- utilization_pct (booked seats / plane capacity)
- trip duration (in minutes)

This model is especially useful for:
- Route optimization
- Aircraft utilization analysis
- Monitoring business KPIs like DAUs, revenue per trip, load factor

It complements `fact_orders` by providing the **supply-side** and **trip-level**
view of operations, while `fact_orders` focuses on individual customer behavior.
================================================================================
*/
WITH

-- Base trip information
trips AS (
    SELECT
        trip_id
        , airplane_id
        , start_timestamp
        , end_timestamp
        , origin_city
        , destination_city
    FROM
        {{ ref('stg_boltic_trips') }}
    {% if is_dev_env() %}
    LIMIT 100
    {% endif %}
)

-- Aggregated order metrics per trip
, orders AS (
    SELECT
        trip_id
        , COUNT(DISTINCT order_id) AS total_orders
        , COUNT(DISTINCT seat_number) AS seats_booked
        , SUM(price_eur) AS trip_revenue
    FROM
        {{ ref('stg_boltic_orders') }}
    GROUP BY
        trip_id
)

-- Plane capacity lookup
, dim_planes AS (
    SELECT
          airplane_id
        , max_seats
    FROM
        {{ ref('dim_planes') }}
)

-- Final enriched join
, joined AS (
    SELECT
        t.trip_id
        , t.airplane_id
        , t.start_timestamp
        , t.end_timestamp
        , t.origin_city
        , t.destination_city
        , TIMESTAMPDIFF(MINUTE, t.start_timestamp, t.end_timestamp) AS duration_min
        , o.total_orders
        , o.seats_booked
        , o.trip_revenue
        , d.max_seats
        , ROUND(CASE WHEN d.max_seats > 0 THEN o.seats_booked / d.max_seats ELSE NULL END, 2) AS utilization_pct
    FROM
        trips t
    LEFT JOIN
        orders AS o
        ON
            t.trip_id = o.trip_id
    LEFT JOIN
        dim_planes d
        ON
            t.airplane_id = d.airplane_id
)

-- Deduplication for idempotency
, deduped AS (
    SELECT *
    FROM
        joined
    QUALIFY
        ROW_NUMBER() OVER (PARTITION BY trip_id ORDER BY trip_id) = 1
)

SELECT * FROM deduped