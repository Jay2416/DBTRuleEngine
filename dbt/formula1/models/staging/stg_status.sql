with source as (
    select * from {{ source('raw_data', 'status') }}
),

renamed_and_casted as (
    select
        cast(statusId as integer) as status_id,
        cast(status as string) as status_description
    from source
)

select * from renamed_and_casted