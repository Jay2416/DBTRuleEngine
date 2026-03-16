{{ config(materialized='ephemeral') }}

{{ rule_engine(
    tables=['final_f1_2023_races', 'stg_results'],
    joins=[{'type': 'INNER JOIN', 'table': 'stg_results', 'left': 'final_f1_2023_races.race_id', 'right': 'stg_results.race_id'}],
    select_columns=['stg_results.constructor_id', 'stg_results.points_scored'],
    aggregations=[],
    where_filters=[],
    group_by=[],
    having=[],
    order_by=[],
    limit_rows=None
) }}