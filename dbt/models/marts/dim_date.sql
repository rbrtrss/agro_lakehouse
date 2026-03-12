with spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('1993-01-01' as date)",
        end_date="cast('2031-01-01' as date)"
    ) }}
)

select
    date_day                                             as date_key,
    year(date_day)                                       as year,
    quarter(date_day)                                    as quarter,
    month(date_day)                                      as month,
    week(date_day)                                       as week,
    concat(
        cast(year(date_day) as varchar),
        '-Q',
        cast(quarter(date_day) as varchar)
    )                                                    as year_quarter
from spine
