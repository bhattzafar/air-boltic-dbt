{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='order_id',
        on_schema_change='sync_all_columns',
        cluster_by=['trip_id']
    )
}}

 /*
================================================================================
MODEL: fact_orders
--------------------------------------------------------------------------------
Grain: 1 row per order_id

This model captures customer-level transactional data, joining order events with
customer profiles and trip metadata. It provides a granular view of how users
interact with the Air Boltic platform.

It helps answer:
- Who is booking trips, and how often?
- What do different customer groups pay per seat?
- What is the revenue distribution by customer type or region?
- How do booking behaviors vary by trip type?

Key attributes include:
- price_eur (revenue per order)
- seat_number (seat-level tracking)
- booking timestamp
- customer_group and metadata (segment, email domain, country)

This model is critical for:
- Growth analysis by customer group or segment
- Pricing strategy and revenue attribution
- Evaluating user acquisition and retention (e.g. repeat customers)

In contrast to `fact_trips`, which summarizes trip-level performance,
`fact_orders` provides the **demand-side**, customer-centric lens necessary
for driving growth and personalizing services.
================================================================================
*/

WITH

-- Load order-level transactional data from staging
orders AS (
    SELECT
        order_id
        , trip_id
        , customer_id
        , seat_number
        , price_eur
        , status
    FROM
        {{ ref('stg_boltic_orders') }}
    {% if is_dev_env() %}
    LIMIT 100
    {% endif %}
)

-- Enrich with customer dimension data (grouping, segmentation, etc.)
, dim_customers AS (
    SELECT
        customer_id
        , customer_group_id
        , customer_group_name
        , email_domain
        , customer_country
    FROM
        {{ ref('dim_customers') }}
)

-- Add flight-level context from fact_trips (trip metadata)
, dim_trips AS (
    SELECT
        trip_id
        , airplane_id
        , origin_city
        , destination_city
        , start_timestamp AS booking_ts   -- Use trip start time as booking timestamp
    FROM
        {{ ref('fact_trips') }}
)

, get_plane_segments AS (
    SELECT
        airplane_id
        , manufacturer AS airplane_manufacturer
        , plane_size_segment
        , plane_distance_segment
    FROM
        {{ ref('dim_planes') }}
)

-- Final join: enrich order facts with customer + trip metadata
, joined AS (
    SELECT
        o.order_id
        , o.trip_id
        , o.customer_id
        , o.seat_number
        , o.price_eur
        , o.status
        , t.booking_ts
        , t.airplane_id
        , t.origin_city
        , t.destination_city
        , c.customer_group_id
        , c.customer_group_name
        , c.email_domain
        , c.customer_country
        , p.airplane_manufacturer
        , p.plane_size_segment
        , p.plane_distance_segment
    FROM
        orders AS o
    LEFT JOIN
        dim_trips AS t
        ON
            o.trip_id = t.trip_id
    LEFT JOIN
        dim_customers AS c
        ON
            o.customer_id = c.customer_id
    LEFT JOIN
        get_plane_segments AS p
        ON
            p.airplane_id = t.airplane_id
)

-- Deduplicate based on primary key
, deduped AS (
    SELECT *
    FROM
        joined
    QUALIFY
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_id) = 1
)

SELECT * FROM deduped
