{% test test_metric_is_trending_up(model, metric) %}
-- Test: Metric should trend upward or stay flat across 7d → 15d → 30d

SELECT
    destination_city
FROM {{ model }}
WHERE
    {{ metric }}_7d > {{ metric }}_15d
    OR {{ metric }}_15d > {{ metric }}_30d
{% endtest %}