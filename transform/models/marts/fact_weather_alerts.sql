with staging as (
    select * from {{ ref('stg_weather_data') }}
)

select
    *,
    -- Logic for Citizen Alerts
    case
        when current_temp > 38 then 'EXTREME HEAT'
        when current_temp > 32 and humidity_pct > 70 then 'HIGH HUMIDITY RISK'
        when sky_condition in ('Thunderstorm', 'Squall', 'Tornado') then 'SEVERE WEATHER'
        else 'CLEAR/STABLE'
    end as alert_level,

    -- Calculating the "Heat Gap" (Difference between actual and perceived)
    round(perceived_temp - current_temp, 2) as heat_gap
from staging