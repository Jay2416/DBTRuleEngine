with source as (
    select * from {{ source('raw_data', 'drivers') }}
),

renamed_and_casted as (
    select
        -- Primary Key
        cast(driverId as integer) as driver_id,

        -- Attributes
        cast(driverRef as string) as driver_ref,
        -- Handle \N for driver number
        cast(nullif(number, '\\N') as integer) as driver_number,
        cast(code as string) as driver_code,
        cast(forename as string) as first_name,
        cast(surname as string) as last_name,
        cast(dob as date) as date_of_birth,
        cast(nationality as string) as nationality,
        cast(url as string) as wikipedia_url

    from source
)

select * from renamed_and_casted