{% macro join_country_code(cte_name, column_name) %}
, get_country_code AS (
    SELECT
        {{ cte_name }}.*,
        cc.country
    FROM
        {{ cte_name }}
    LEFT JOIN {{ ref('country_code') }} AS cc
        ON {{ cte_name }}.{{ column_name }} = cc.country_code
)
{% endmacro %}