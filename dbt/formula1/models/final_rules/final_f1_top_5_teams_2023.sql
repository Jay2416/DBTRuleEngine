{{ config(materialized='table') }}

{{ rule_engine(
    tables=['final_f1_2023_results', 'stg_constructors'],
    joins=[{'type': 'INNER JOIN', 'table': 'stg_constructors', 'left': 'final_f1_2023_results.constructor_id', 'right': 'stg_constructors.constructor_id'}],
    select_columns=['stg_constructors.constructor_name'],
    aggregations=[{'func': 'SUM', 'col': 'final_f1_2023_results.points_scored', 'alias': 'total_points'}],
    where_filters=[],
    group_by=['stg_constructors.constructor_name'],
    having=[],
    order_by=[],
    limit_rows=5
) }}