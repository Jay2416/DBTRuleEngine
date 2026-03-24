{{ config(materialized='table', schema='final_rules', alias='driver_test') }}

{{ non_sequential_rule_engine(
    rule_models=['is_race_winner', 'is_pole_sitter'],
    key_column='driver_id',
    remark_column='remark'
) }}