{{ config(materialized='table', file_format='iceberg') }}

with source_data as (
    select * from {{ source('raw_data', 'dim_customer') }}
),

transformed_data as (
    select 
        customer_sk,
        customer_code,
        last_name
    from source_data
)

-- You MUST add this final select to return the data
select * from transformed_data