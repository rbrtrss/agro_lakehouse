with source as (
    select * from {{ source('silver', 'senasa_certs') }}
)

select
    cast(year as integer)                          as year,
    lower(trim(provincia))                         as province,
    lower(trim(pais_destino))                      as country,
    pais_destino_id_iso_3166_1                     as country_iso2,
    lower(trim(mercaderia_certificada))            as commodity,
    cast(tn as double)                             as total_tn
from source
