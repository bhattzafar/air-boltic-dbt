{{
    config(
        materialized='view'
    )
}}

SELECT * FROM {{ref('int_success_summary')}}