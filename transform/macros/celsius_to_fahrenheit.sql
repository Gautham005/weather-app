{% macro celsius_to_fahrenheit(temp_c_col, precision=1) %}
    round(({{ temp_c_col }} * 9/5) + 32, {{ precision }})
{% endmacro %}