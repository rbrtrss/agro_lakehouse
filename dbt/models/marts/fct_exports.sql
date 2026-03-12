with exports as (
    select * from {{ ref('int_exports_enriched') }}
),

dim_date as (
    select date_key, year
    from {{ ref('dim_date') }}
    where month(date_key) = 1 and day(date_key) = 1
),

dim_destination as (
    select destination_key, country
    from {{ ref('dim_destination') }}
),

dim_product as (
    select product_key, commodity
    from {{ ref('dim_product') }}
),

dim_province as (
    select province_key, province
    from {{ ref('dim_province') }}
),

fact as (
    select
        e.year,
        e.province,
        e.country,
        e.commodity,
        e.total_tn,
        e.fob_usd,
        e.total_fob_usd,
        d.date_key,
        dest.destination_key,
        prod.product_key,
        prov.province_key
    from exports e
    left join dim_date d
        on e.year = d.year
    left join dim_destination dest
        on e.country = dest.country
    left join dim_product prod
        on e.commodity = prod.commodity
    left join dim_province prov
        on e.province = prov.province
)

select
    {{ dbt_utils.generate_surrogate_key(['year', 'province', 'country', 'commodity']) }}
                            as export_key,
    date_key,
    destination_key,
    product_key,
    province_key,
    year,
    total_tn,
    fob_usd,
    total_fob_usd
from fact
