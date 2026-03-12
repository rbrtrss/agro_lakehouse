with current_records as (
    select * from {{ ref('snap_destination') }}
    where dbt_valid_to is null
),

deduped as (
    select
        *,
        row_number() over (partition by country_iso2 order by dbt_updated_at desc) as rn
    from current_records
)

select
    {{ dbt_utils.generate_surrogate_key(['country_iso2']) }} as destination_key,
    country_iso2,
    country,
    continent
from deduped
where rn = 1
