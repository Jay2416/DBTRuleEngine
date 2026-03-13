with source as (
    select * from {{ source('raw_data', 'constructor_results') }}
),

renamed_and_casted as (
    select
        cast(constructorResultsId as integer) as constructor_results_id,
        cast(raceId as integer) as race_id,
        cast(constructorId as integer) as constructor_id,
        cast(points as numeric) as points_scored,
        cast(nullif(status, '\\N') as string) as status_code
    from source
)

select * from renamed_and_casted