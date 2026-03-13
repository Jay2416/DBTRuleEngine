with source as (
    select * from {{ source('raw_data', 'pit_stops') }}
),

renamed_and_casted as (
    select
        cast(raceId as integer) as race_id,
        cast(driverId as integer) as driver_id,
        cast(stop as integer) as pit_stop_number,
        cast(lap as integer) as lap_number,
        cast(time as string) as pit_stop_time,
        cast(duration as string) as pit_stop_duration,
        cast(milliseconds as integer) as pit_stop_duration_ms
    from source
)

select * from renamed_and_casted