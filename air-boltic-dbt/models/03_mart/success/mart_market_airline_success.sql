{{
    config(
        materialized='view'
    )
}}

SELECT * FROM {{ref('int_airplane_market_success')}}