with source as (
    select * from {{ source('raw_data', 'circuits') }}
),

renamed_and_casted as (
    select
        cast(circuitId as integer) as circuit_id,
        cast(circuitRef as string) as circuit_ref,
        cast(name as string) as circuit_name,
        cast(location as string) as location,
        cast(country as string) as country,
        cast(lat as float) as latitude,
        cast(lng as float) as longitude,
        cast(nullif(alt, '\\N') as integer) as altitude,
        cast(url as string) as wikipedia_url
    from source
)

select * from renamed_and_casted