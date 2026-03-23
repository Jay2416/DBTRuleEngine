{{ config(materialized='table', schema='final_rules', alias='front_grid_starters') }}

{{ non_sequential_rule_engine(
    rule_models=['p2_starters', 'pole_sitters', 'p3_starters'],
    key_column='race_id',
    target_column='driver_id'
) }}