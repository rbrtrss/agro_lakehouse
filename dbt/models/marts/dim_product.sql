with commodities as (
    select distinct lower(trim(commodity)) as commodity
    from {{ ref('stg_senasa_certs') }}
    where commodity is not null
),

categorized as (
    select
        commodity,
        case
            when commodity in ('soja', 'girasol', 'maní', 'colza', 'cártamo')
                then 'Oilseeds'
            when commodity in ('trigo', 'maíz', 'cebada', 'sorgo', 'avena', 'centeno', 'arroz')
                then 'Grains'
            when commodity in ('carne bovina', 'carne porcina', 'carne ovina', 'miel', 'lana', 'cuero')
                then 'Livestock'
            when commodity in ('limón', 'naranja', 'mandarina', 'pomelo')
                then 'Citrus'
            when commodity in ('manzana', 'pera', 'uva', 'cereza', 'arándano', 'frutilla')
                then 'Fruits'
            else 'Other'
        end as product_category,
        case
            when commodity = 'soja'             then 'Soybean'
            when commodity = 'girasol'          then 'Sunflower'
            when commodity = 'maíz'             then 'Corn'
            when commodity = 'trigo'            then 'Wheat'
            when commodity = 'cebada'           then 'Barley'
            when commodity = 'sorgo'            then 'Sorghum'
            when commodity = 'maní'             then 'Peanut'
            when commodity = 'colza'            then 'Canola'
            when commodity = 'arroz'            then 'Rice'
            when commodity = 'carne bovina'     then 'Beef'
            when commodity = 'carne porcina'    then 'Pork'
            when commodity = 'carne ovina'      then 'Lamb'
            when commodity = 'miel'             then 'Honey'
            when commodity = 'lana'             then 'Wool'
            when commodity = 'cuero'            then 'Leather'
            when commodity = 'limón'            then 'Lemon'
            when commodity = 'naranja'          then 'Orange'
            when commodity = 'mandarina'        then 'Tangerine'
            when commodity = 'manzana'          then 'Apple'
            when commodity = 'pera'             then 'Pear'
            when commodity = 'uva'              then 'Grape'
            when commodity = 'cereza'           then 'Cherry'
            when commodity = 'arándano'         then 'Blueberry'
            else commodity
        end as commodity_english
    from commodities
)

select
    {{ dbt_utils.generate_surrogate_key(['commodity']) }} as product_key,
    commodity,
    product_category,
    commodity_english
from categorized
