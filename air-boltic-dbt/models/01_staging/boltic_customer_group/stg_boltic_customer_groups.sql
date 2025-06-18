{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='customer_group_id',
        on_schema_change='sync_all_columns',
        cluster_by=['group_type']
    )
}}

WITH

customer_groups AS (
    SELECT
        CAST(id AS STRING) AS customer_group_id
        , LOWER( CAST(type AS STRING)) AS group_type
        , LOWER( CAST(name AS STRING)) AS customer_group_name
        , CAST(registry_number AS STRING) AS group_registry_number
    FROM
        {{ source('air_boltic_data', 'air_boltic_data_customer_group') }}
    {% if is_dev_env() %}
        LIMIT 100
    {% endif %}
)

SELECT * FROM customer_groups



