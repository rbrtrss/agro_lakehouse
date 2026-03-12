with source as (
    select * from {{ source('silver', 'worldbank_indicators') }}
    where country_code = 'ARG'
)

select
    indicator_code                    as indicator_code,
    indicator_name                    as indicator_name,
    cast(year as integer)             as year,
    cast(value as double)             as indicator_value
from source
