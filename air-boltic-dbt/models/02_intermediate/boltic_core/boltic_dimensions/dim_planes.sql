{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='airplane_id',
        on_schema_change='sync_all_columns',
        cluster_by=['manufacturer']
    )
}}

WITH

airplane_id AS (
    SELECT
        airplane_id
        , airplane_model
        , manufacturer
    FROM
        {{ ref('stg_boltic_planes') }}
    {% if is_dev_env() %}
        LIMIT 100
    {% endif %}
)

, airplane_details AS (
    SELECT
        airplane_details_id
        , make
        , model
        , engine_type
        , max_distance
        , max_seats
        , max_weight
    FROM
        {{ ref('stg_plane_details') }}
    {% if is_dev_env() %}
        LIMIT 100
    {% endif %}
)

, join_plane_details AS (
    SELECT
        ar.airplane_id
        , ar.airplane_model
        , ar.manufacturer
        , dt.make
        , dt.model
        , dt.engine_type
        , dt.max_distance
        , dt.max_seats
        , dt.max_weight
    FROM
        airplane_id AS ar
    LEFT JOIN
        airplane_details AS dt
        ON
            LOWER(ar.manufacturer) = LOWER(dt.make)
            AND LOWER(ar.airplane_model) = LOWER(dt.model)
)

, remove_duplicates AS (
    SELECT *
    FROM
        join_plane_details
    QUALIFY
        ROW_NUMBER() OVER (PARTITION BY airplane_id, manufacturer, model ORDER BY airplane_id) = 1
)

, adding_segments AS (
    SELECT
        *
        , CASE
            WHEN max_seats > AVG(max_seats) OVER () THEN 'big_plane'
            ELSE 'small_plane'
        END AS plane_size_segment
        , CASE
            WHEN max_distance > AVG(max_distance) OVER () THEN 'long_hauler'
            ELSE 'small_hauler'
        END AS plane_distance_segment
    FROM
        remove_duplicates
)

SELECT * FROM adding_segments
