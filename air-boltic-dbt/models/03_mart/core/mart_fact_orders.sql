{{
    config(
        materialized='view'
    )
}}

SELECT * FROM {{ref('fact_orders')}}