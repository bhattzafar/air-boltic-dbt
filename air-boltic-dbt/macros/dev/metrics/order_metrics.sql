{% macro air_boltic_order_metrics_window(days, group_col='destination_city') %}
(
    SELECT
        o.{{ group_col }}
        , COUNT(*) AS total_orders_{{ days }}d
        , SUM(price_eur) AS total_revenue_{{ days }}d
        , AVG(price_eur) AS avg_ticket_price_{{ days }}d
        , SUM(CASE WHEN status = 'confirmed' THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS pct_confirmed_orders_{{ days }}d
        , (COUNT(*) - COUNT(DISTINCT customer_id)) * 1.0 / COUNT(*) AS repeat_customer_pct_{{ days }}d
    FROM
        {{ ref('fact_orders') }} AS o
    LEFT JOIN
        {{ ref('fact_trips') }} AS t
        ON
            o.trip_id = t.trip_id
    WHERE
        start_timestamp >= DATEADD(day, -{{ days }}, CURRENT_DATE)
    GROUP BY
        1
)
{% endmacro %}