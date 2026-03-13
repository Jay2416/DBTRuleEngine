with source as (
    select * from {{ source('raw_data', 'results') }}
),

renamed_and_casted as (
    select
        -- Primary Key
        cast(resultId as integer) as result_id,

        -- Foreign Keys
        cast(raceId as integer) as race_id,
        cast(driverId as integer) as driver_id,
        cast(constructorId as integer) as constructor_id,
        cast(statusId as integer) as status_id,

        -- Race Details (Handling \N values)
        cast(nullif(number, '\\N') as integer) as car_number,
        cast(grid as integer) as grid_position,
        cast(nullif(position, '\\N') as integer) as finishing_position,
        cast(positionText as string) as position_text,
        cast(positionOrder as integer) as position_order,
        cast(points as numeric) as points_scored,
        cast(laps as integer) as laps_completed,
        
        -- Times and Speeds
        cast(nullif(time, '\\N') as string) as race_time,
        cast(nullif(milliseconds, '\\N') as integer) as race_time_ms,
        cast(nullif(fastestLap, '\\N') as integer) as fastest_lap_number,
        cast(nullif(rank, '\\N') as integer) as fastest_lap_rank,
        cast(nullif(fastestLapTime, '\\N') as string) as fastest_lap_time,
        cast(nullif(fastestLapSpeed, '\\N') as float) as fastest_lap_speed

    from source
)

select * from renamed_and_casted