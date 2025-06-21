{% macro air_boltic_trip_metrics_window(days, group_col='destination_city') %}
(
    SELECT
        {{ group_col }}
        , COUNT(DISTINCT trip_id) AS trip_count_{{ days }}d
        , AVG(utilization_pct) AS avg_utilization_pct_{{ days }}d
    FROM
        {{ ref('fact_trips') }}
    WHERE
        start_timestamp >= DATEADD(day, -{{ days }}, CURRENT_DATE)
    GROUP BY
        1
)
{% endmacro %}