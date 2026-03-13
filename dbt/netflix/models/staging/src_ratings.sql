with raw_rating as (
    select * from {{ source('raw_data', 'ratings') }}
)

select 
    userid as user_id,
    movieid as movie_id,
    rating,
    TO_TIMESTAMP_LTZ(timestamp) as rating_timestamp
from raw_rating
