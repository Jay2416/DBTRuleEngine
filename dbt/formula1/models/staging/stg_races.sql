with source as (
    select * from {{ source('raw_data', 'races') }}
),

renamed_and_casted as (
    select
        cast(raceId as integer) as race_id,
        cast(year as integer) as race_year,
        cast(round as integer) as race_round,
        cast(circuitId as integer) as circuit_id,
        cast(name as string) as race_name,
        cast(date as date) as race_date,
        cast(nullif(time, '\\N') as string) as race_time,
        cast(url as string) as wikipedia_url,
        
        -- Practice and Qualifying Sessions
        cast(nullif(fp1_date, '\\N') as date) as free_practice_1_date,
        cast(nullif(fp1_time, '\\N') as string) as free_practice_1_time,
        cast(nullif(fp2_date, '\\N') as date) as free_practice_2_date,
        cast(nullif(fp2_time, '\\N') as string) as free_practice_2_time,
        cast(nullif(fp3_date, '\\N') as date) as free_practice_3_date,
        cast(nullif(fp3_time, '\\N') as string) as free_practice_3_time,
        cast(nullif(quali_date, '\\N') as date) as qualifying_date,
        cast(nullif(quali_time, '\\N') as string) as qualifying_time,
        cast(nullif(sprint_date, '\\N') as date) as sprint_date,
        cast(nullif(sprint_time, '\\N') as string) as sprint_time
    from source
)

select * from renamed_and_casted