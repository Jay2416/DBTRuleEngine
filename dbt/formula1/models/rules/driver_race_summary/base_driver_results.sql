{{ config(materialized='table') }}

{{ rule_engine(
    tables=['stg_results', 'stg_drivers'], joins=[{'type': 'INNER JOIN', 'table': 'stg_drivers', 'left': 'stg_results.driver_id', 'right': 'stg_drivers.driver_id'}], select_columns=['stg_results.race_id', 'stg_results.finishing_position', 'stg_results.points_scored', 'stg_drivers.driver_id', 'stg_drivers.last_name'],
    aggregations=[], where_filters=[], group_by=[],
    having=[], order_by=[], limit_rows=None
) }}