{{ config(materialized='table') }}

{{ non_sequential_rule_engine(
    rule_models=['final_gold_finishes', 'final_silver_finishes', 'final_bronze_finishes'],
    key_column='driver_id',
    target_column='race_id'
) }}