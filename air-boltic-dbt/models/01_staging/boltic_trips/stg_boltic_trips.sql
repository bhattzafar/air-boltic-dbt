{{
    config(
        materialized='incremental',
        incremental_strategy = 'merge',
        cluster_by = ['origin_country', 'destination_country'],
        unique_key='trip_id',
        on_schema_change='sync_all_columns',
    )
}}

WITH

trips AS (
    SELECT
        CAST(trip_id AS STRING) AS trip_id
        , CAST(origin_city AS STRING) AS origin_city
        , CAST(destination_city AS STRING) AS destination_city
        , CAST(airplane_id AS STRING) AS airplane_id
        , CAST(start_timestamp AS TIMESTAMP) AS start_timestamp
        , CAST(end_timestamp AS TIMESTAMP) AS end_timestamp
        , DATE_TRUNC('DD', CAST(start_timestamp AS DATE)) AS trip_date
        , DATE_TRUNC('MM', CAST(start_timestamp AS DATE)) AS trip_month
        , DATE_TRUNC('YEAR', CAST(start_timestamp AS DATE)) AS trip_year
    FROM
        {{ source('air_boltic_data', 'air_boltic_data_trip') }}
    {% if is_dev_env() %}
        LIMIT 100
    {% endif %}
)

SELECT * FROM trips
