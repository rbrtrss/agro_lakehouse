with senasa_agg as (
    select
        year,
        province,
        country,
        commodity,
        sum(total_tn) as commodity_tn
    from {{ ref('stg_senasa_certs') }}
    group by year, province, country, commodity
),

senasa_totals as (
    select
        year,
        province,
        country,
        sum(commodity_tn) as total_tn_for_group
    from senasa_agg
    group by year, province, country
),

senasa_with_share as (
    select
        s.year,
        s.province,
        s.country,
        s.commodity,
        s.commodity_tn,
        t.total_tn_for_group,
        s.commodity_tn / nullif(t.total_tn_for_group, 0) as tn_share
    from senasa_agg s
    join senasa_totals t
        on s.year = t.year
        and s.province = t.province
        and s.country = t.country
),

indec_agg as (
    select
        year,
        province,
        country,
        sum(fob_usd)   as total_fob_usd
    from {{ ref('stg_indec_exports') }}
    group by year, province, country
),

joined as (
    select
        s.year,
        s.province,
        s.country,
        s.commodity,
        s.commodity_tn                                          as total_tn,
        s.tn_share * coalesce(i.total_fob_usd, 0)              as fob_usd,
        coalesce(i.total_fob_usd, 0)                           as total_fob_usd
    from senasa_with_share s
    left join indec_agg i
        on s.year = i.year
        and s.province = i.province
        and s.country = i.country
)

select * from joined
