with source as (
    select * from {{ source('raw_data', 'lap_times') }}
),

renamed_and_casted as (
    select
        cast(raceId as integer) as race_id,
        cast(driverId as integer) as driver_id,
        cast(lap as integer) as lap_number,
        cast(position as integer) as lap_position,
        cast(time as string) as lap_time,
        cast(milliseconds as integer) as lap_time_ms
    from source
)

select * from renamed_and_casted