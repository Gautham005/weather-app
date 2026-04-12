with source as (
    select * from {{ source('weather_api', 'weather_stream') }}
),

silver as
(select
    location_id,
    upper(location_name) as city_name,
    temp_celsius as current_temp_c,
    {{ celsius_to_fahrenheit('temp_celsius') }} as current_temp_f,
    feels_like as perceived_temp_c,
    {{ celsius_to_fahrenheit('feels_like') }} as perceived_temp_f,
    {{ classify_temp('temp_celsius') }} as temp_severity,
    humidity as humidity_pct,
    weather_main as sky_condition,
    cast(recorded_at as timestamp) as recorded_at
from source)

select * from silver