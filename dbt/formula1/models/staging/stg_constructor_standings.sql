with source as (
    select * from {{ source('raw_data', 'constructor_standings') }}
),

renamed_and_casted as (
    select
        cast(constructorStandingsId as integer) as constructor_standings_id,
        cast(raceId as integer) as race_id,
        cast(constructorId as integer) as constructor_id,
        cast(points as numeric) as championship_points,
        cast(position as integer) as standing_position,
        cast(positionText as string) as standing_position_text,
        cast(wins as integer) as season_wins
    from source
)

select * from renamed_and_casted