with staging as (
    select * from {{ ref('stg_weather_data') }}
)

select
    *,
    {{ generate_alert('current_temp_c', 'humidity_pct', 'sky_condition') }} as alert_level,
    round(perceived_temp_c - current_temp_c, 2) as heat_gap_c,
    round(perceived_temp_f - current_temp_f, 2) as heat_gap_f,
from staging