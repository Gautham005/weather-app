{% macro generate_alert(temp_c_col, humidity_col, condition_col) %}
    case
        when {{ temp_c_col }} > 38 then 'Extreme Heat'
        when {{ temp_c_col }} > 32 and {{ humidity_col }} > 70 then 'High Humidity Risk'
        when {{ condition_col }} in ('Thunderstorm', 'Squall', 'Tornado') then 'Severe Weather'
        else 'Stable'
    end
{% endmacro %}