with source as (
    select * from {{ source('raw_data', 'driver_standings') }}
),

renamed_and_casted as (
    select
        cast(driverStandingsId as integer) as driver_standings_id,
        cast(raceId as integer) as race_id,
        cast(driverId as integer) as driver_id,
        cast(points as numeric) as championship_points,
        cast(position as integer) as standing_position,
        cast(positionText as string) as standing_position_text,
        cast(wins as integer) as season_wins
    from source
)

select * from renamed_and_casted