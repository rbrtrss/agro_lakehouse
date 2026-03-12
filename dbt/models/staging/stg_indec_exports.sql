with source as (
    select * from {{ source('silver', 'indec_exports') }}
)

select
    cast(year as integer)              as year,
    lower(trim(province))             as province,
    lower(trim(country))              as country,
    cast(fob_usd as double)           as fob_usd
from source
