{% macro calculate_surge_impact(surge_multiplier, actual_fare) %}
    CASE 
        WHEN {{ surge_multiplier }} <= 1.0 THEN 'No Surge'
        WHEN {{ surge_multiplier }} <= 1.5 THEN 'Low Surge'
        WHEN {{ surge_multiplier }} <= 2.0 THEN 'Medium Surge'
        WHEN {{ surge_multiplier }} <= 3.0 THEN 'High Surge'
        ELSE 'Extreme Surge'
    END
{% endmacro %}

{% macro is_extreme_surge(surge_multiplier) %}
    CASE 
        WHEN {{ surge_multiplier }} > 10 THEN TRUE
        ELSE FALSE
    END
{% endmacro %}

{% macro calculate_net_revenue(actual_fare, fee) %}
    COALESCE({{ actual_fare }}, 0) - COALESCE({{ fee }}, 0)
{% endmacro %}