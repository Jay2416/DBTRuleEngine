with raw_movies as (
    select * from {{ source('raw_data', 'movies') }}
)

select 
    movieid as movie_id,
    title,
    genres
from raw_movies