{% macro classify_temp(temp_c_col) %}
    case
        when {{ temp_c_col }} >= 35 then 'Extreme'
        when {{ temp_c_col }} >= 30 then 'High'
        when {{ temp_c_col }} >= 20 then 'Moderate'
        else 'Low'
    end
{% endmacro %}