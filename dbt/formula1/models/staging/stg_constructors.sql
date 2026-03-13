with source as (
    select * from {{ source('raw_data', 'constructors') }}
),

renamed_and_casted as (
    select
        cast(constructorId as integer) as constructor_id,
        cast(constructorRef as string) as constructor_ref,
        cast(name as string) as constructor_name,
        cast(nationality as string) as nationality,
        cast(url as string) as wikipedia_url
    from source
)

select * from renamed_and_casted