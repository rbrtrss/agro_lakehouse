with source as (
    select * from {{ source('silver', 'indec_exports') }}
)

select
    cast(year as integer)              as year,
    lower(trim(province))             as province,
    lower(trim(country))              as country,
    lower(trim(commodity))            as commodity,
    cast(fob_usd as double)           as fob_usd,
    cast(total_tn as double)          as total_tn
from source
