{{
    config(
        materialized='view'
    )
}}

SELECT * FROM {{ref('int_market_success')}}