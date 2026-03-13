with raw_genome_scores as (
    select * from {{ source('raw_data', 'genome_scores') }}
)

select 
    movieid as movie_id,
    tagid as tag_id,
    relevance
from raw_genome_scores

