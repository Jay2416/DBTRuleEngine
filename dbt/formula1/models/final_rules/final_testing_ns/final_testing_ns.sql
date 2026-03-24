{{ config(materialized='table', schema='final_rules', alias='testing_ns') }}

{{ non_sequential_rule_engine(
    rule_models=['race_winner', 'points_scored'],
    key_column='driver_id',
    remark_column='remark'
) }}