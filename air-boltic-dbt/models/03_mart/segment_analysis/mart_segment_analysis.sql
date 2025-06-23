{{
    config(
        materialized='view'
    )
}}

SELECT * FROM {{ref('int_customer_segment_analysis')}}