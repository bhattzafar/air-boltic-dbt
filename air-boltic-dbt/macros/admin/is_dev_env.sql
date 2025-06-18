/*
    Assuming the dev environment is hosted in a separate schema name i.e. boltic_data_dev
    This macro allows developers to limit the amount of data being processed in dev model
    resulting in reduced consumption of resources.
*/

{% macro is_dev_env() %}
    {% if target.project|lower == "boltic_data_dev" and var("apply_limit", default=True) %}
        {% set is_dev_env = True %}

    {% else %} {% set is_dev_env = False %}
    {% endif %}

    {{ return(is_dev_env) }}

{% endmacro %}