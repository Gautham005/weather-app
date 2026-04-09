with source as (
    select * from {{ ref('raw_weather_monitor') }}
)

select
    -- Standardizing IDs and Names
    location_id,
    upper(location_name) as city_name,

    -- Ensuring Coordinates are Floats for the UI
    cast(latitude as float) as lat,
    cast(longitude as float) as lon,

    -- Weather Metrics
    temp_celsius as current_temp,
    feels_like as perceived_temp,
    humidity as humidity_pct,
    weather_main as sky_condition,

    -- Time Handling
    cast(ingested_at as timestamp) as observation_time
from source