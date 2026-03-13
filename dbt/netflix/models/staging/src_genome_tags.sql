with raw_genome_tag as (
    select * from {{ source('raw_data', 'genome_tags') }}
)

select 
    tagid as tag_id,
    tag
from raw_genome_tag