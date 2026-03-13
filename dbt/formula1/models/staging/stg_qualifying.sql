with source as (
    select * from {{ source('raw_data', 'qualifying') }}
),

renamed_and_casted as (
    select
        cast(qualifyId as integer) as qualify_id,
        cast(raceId as integer) as race_id,
        cast(driverId as integer) as driver_id,
        cast(constructorId as integer) as constructor_id,
        cast(nullif(number, '\\N') as integer) as car_number,
        cast(position as integer) as qualifying_position,
        cast(nullif(q1, '\\N') as string) as q1_lap_time,
        cast(nullif(q2, '\\N') as string) as q2_lap_time,
        cast(nullif(q3, '\\N') as string) as q3_lap_time
    from source
)

select * from renamed_and_casted