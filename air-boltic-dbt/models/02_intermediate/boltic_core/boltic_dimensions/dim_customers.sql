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
        customer_id
        , customer_name
        , customer_group_id
        , country_code
        , email_domain
    FROM {{ ref('stg_boltic_customers_non_pii') }}
    {% if is_dev_env() %}
        LIMIT 100
    {% endif %}
)

, customer_groups AS (
    SELECT
        customer_group_id
        , customer_group_name
    FROM
        {{ ref('stg_boltic_customer_groups') }}
    {% if is_dev_env() %}
        LIMIT 100
    {% endif %}
)

, join_data AS (
    SELECT
        customers.customer_id
        , customers.customer_name
        , customers.customer_group_id
        , customers.email_domain
        , customers.country_code
        , customer_groups.customer_group_name
    FROM
        customers AS customers
    LEFT JOIN
        customer_groups AS customer_groups 
        ON
            customers.customer_group_id = customer_groups.customer_group_id
)

{{ join_country_code('join_data', 'country_code') }}

, remove_duplicates AS (
    SELECT
        customer_id
        , customer_name
        , customer_group_id
        , email_domain
        , country_code
        , customer_group_name
        , country AS customer_country
    FROM
        get_country_code
    QUALIFY
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY customer_id) = 1
)

SELECT * FROM remove_duplicates