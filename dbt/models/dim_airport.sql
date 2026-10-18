{{ config(materialized='table') }}
with candidates as (
    select
        id,
        coalesce(nullif(trim(icao_code), ''), nullif(trim(ident), '')) as airport_code,
        name as airport_name, iso_country, iso_region, municipality, type as airport_type,
        try_cast(latitude_deg as double) as latitude,
        try_cast(longitude_deg as double) as longitude
    from {{ source('raw', 'airports') }}
), ranked as (
    select *, row_number() over (
        partition by airport_code
        order by case when airport_type = 'closed' then 1 else 0 end, try_cast(id as bigint)
    ) as priority
    from candidates where airport_code is not null
)
select airport_code, airport_name, iso_country, iso_region, municipality,
       airport_type, latitude, longitude
from ranked where priority = 1
