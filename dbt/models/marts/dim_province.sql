with provinces as (
    select distinct lower(trim(province)) as province
    from {{ ref('stg_senasa_certs') }}
    where province is not null
),

enriched as (
    select
        province,
        case
            when province in (
                'buenos aires', 'córdoba', 'santa fe', 'entre ríos', 'la pampa'
            ) then true
            else false
        end as is_pampa_region,
        case
            when province in ('buenos aires', 'la pampa')   then 'wheat'
            when province = 'córdoba'                       then 'soybean'
            when province = 'santa fe'                      then 'soybean'
            when province = 'entre ríos'                    then 'soybean'
            when province = 'mendoza'                       then 'grape'
            when province = 'tucumán'                       then 'lemon'
            when province in ('río negro', 'neuquén')       then 'apple'
            when province = 'misiones'                      then 'yerba mate'
            when province = 'salta'                         then 'tobacco'
            when province = 'chaco'                         then 'cotton'
            else 'mixed'
        end as main_crop
    from provinces
)

select
    {{ dbt_utils.generate_surrogate_key(['province']) }} as province_key,
    province,
    is_pampa_region,
    main_crop
from enriched
