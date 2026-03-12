{% snapshot snap_destination %}

{{ config(
    strategy='check',
    unique_key='country_iso2',
    check_cols=['country', 'continent'],
    target_schema='agro_gold'
) }}

select distinct
    pais_destino_id_iso_3166_1      as country_iso2,
    lower(trim(pais_destino))       as country,
    lower(trim(continente))         as continent
from {{ source('silver', 'senasa_certs') }}
where pais_destino_id_iso_3166_1 is not null

{% endsnapshot %}
