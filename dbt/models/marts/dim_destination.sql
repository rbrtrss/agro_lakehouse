with current_records as (
    select * from {{ ref('snap_destination') }}
    where dbt_valid_to is null
)

select
    {{ dbt_utils.generate_surrogate_key(['country_iso2']) }} as destination_key,
    country_iso2,
    country,
    continent
from current_records
