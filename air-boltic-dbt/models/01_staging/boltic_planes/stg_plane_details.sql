{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='airplane_details_id',
        on_schema_change='sync_all_columns',
        cluster_by=['make']
    )
}}

WITH

airplane_details AS (
    SELECT
        dbt_utils.generate_surrogate_key(['make', 'model', 'engine_type']) AS airplane_details_id
        , LOWER(CAST(make AS STRING)) AS make
        , LOWER(CAST(model AS STRING)) AS model
        , LOWER(CAST(engine_type AS STRING)) AS engine_type
        , CAST(max_distance AS INT) AS max_distance
        , CAST(max_seats AS INT) AS max_seats
        , CAST(max_weight AS INT) AS max_weight
    FROM
        {{ source('air_boltic_data', 'air_boltic_data_aeroplane_details') }}
    {% if is_dev_env() %}
        LIMIT 100
    {% endif %}
)

SELECT * FROM airplane_details

