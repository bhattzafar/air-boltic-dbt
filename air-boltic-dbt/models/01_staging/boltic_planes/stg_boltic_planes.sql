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
        CAST(airplane_id AS STRING) AS airplane_id
        , LOWER(CAST(airplane_model AS STRING)) AS airplane_model
        , LOWER(CAST(manufacturer AS STRING)) AS manufacturer
    FROM
        {{ source('air_boltic_data', 'air_boltic_data_aeroplane') }}
    {% if is_dev_env() %}
        LIMIT 100
    {% endif %}
)

SELECT * FROM airplane_id