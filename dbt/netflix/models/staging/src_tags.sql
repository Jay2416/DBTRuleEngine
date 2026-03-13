with raw_tags as (
    select * from {{ source('raw_data', 'tags') }}
)

select
    userid as user_id,
    movieid as movie_id,
    tag,
    TO_TIMESTAMP_LTZ(timestamp) as tag_timestamp
from raw_tags

