{{
    config(
        materialized='incremental',
        incremental_strategy = 'merge',
        unique_key='order_id',
        on_schema_change='sync_all_columns',
    )
}}

WITH

orders AS (
    SELECT
        CAST(order_id AS STRING) AS order_id
        , CAST(customer_id AS STRING) AS customer_id
        , CAST(trip_id AS STRING) AS trip_id
        , CAST(price_eur AS INT) AS price_eur
        , CAST(seat_no AS STRING) AS seat_number
        , CAST(status AS STRING) AS status
    FROM
        {{ source('air_boltic_data', 'air_boltic_data_order') }}
    {% if is_dev_env() %}
        LIMIT 100
    {% endif %}
)

SELECT * FROM orders

