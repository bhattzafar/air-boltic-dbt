{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='customer_id',
        on_schema_change='sync_all_columns',
        cluster_by=['customer_group_id']
    )
}}

WITH

customers AS (
    SELECT
        CAST(customer_id AS STRING) AS customer_id
        , CAST(name AS STRING) AS customer_name
        , CAST(customer_group_id AS STRING) AS customer_group_id
        , CAST(email AS STRING) AS customer_email
        , CAST(phone_number AS STRING) AS customer_phone_number
    FROM
        {{ source('air_boltic_data', 'air_boltic_data_customer') }}
    {% if is_dev_env() %}
        LIMIT 100
    {% endif %}
)

SELECT * FROM customers


