{{ config(materialized='table') }}

{{ non_sequential_rule_engine(
    rule_models=['p3_starters', 'pole_sitters', 'p2_starters'],
    key_column='race_id',
    target_column='driver_id'
) }}