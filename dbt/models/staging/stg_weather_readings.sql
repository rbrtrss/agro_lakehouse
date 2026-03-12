with source as (
    select * from {{ source('silver', 'weather_daily_readings') }}
)

select
    cast(date as date)                as date,
    lower(trim(province))            as province,
    station_id                       as station_id,
    avg_temp_c                       as avg_temp_c,
    min_temp_c                       as min_temp_c,
    max_temp_c                       as max_temp_c,
    precipitation_mm                 as precipitation_mm
from source
