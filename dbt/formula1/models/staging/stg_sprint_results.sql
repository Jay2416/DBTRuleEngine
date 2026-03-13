with source as (
    select * from {{ source('raw_data', 'sprint_results') }}
),

renamed_and_casted as (
    select
        cast(resultId as integer) as sprint_result_id,
        cast(raceId as integer) as race_id,
        cast(driverId as integer) as driver_id,
        cast(constructorId as integer) as constructor_id,
        cast(nullif(number, '\\N') as integer) as car_number,
        cast(grid as integer) as grid_position,
        cast(nullif(position, '\\N') as integer) as finishing_position,
        cast(positionText as string) as position_text,
        cast(positionOrder as integer) as position_order,
        cast(points as numeric) as points_scored,
        cast(laps as integer) as laps_completed,
        cast(nullif(time, '\\N') as string) as sprint_time,
        cast(nullif(milliseconds, '\\N') as integer) as sprint_time_ms,
        cast(nullif(fastestLap, '\\N') as integer) as fastest_lap_number,
        cast(nullif(fastestLapTime, '\\N') as string) as fastest_lap_time,
        cast(statusId as integer) as status_id
    from source
)

select * from renamed_and_casted