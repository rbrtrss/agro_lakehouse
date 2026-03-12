with source as (
    select * from {{ source('silver', 'weather_daily_readings') }}
)

select
    cast(date as date)                as date,
    lower(trim(province))            as province,
    cast(temp_max_c as double)       as temp_max_c,
    cast(temp_min_c as double)       as temp_min_c,
    cast(precipitation_mm as double) as precipitation_mm
from source
