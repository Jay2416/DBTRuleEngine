with source as (
    select * from {{ source('raw_data', 'seasons') }}
),

renamed_and_casted as (
    select
        cast(year as integer) as season_year,
        cast(url as string) as wikipedia_url
    from source
)

select * from renamed_and_casted