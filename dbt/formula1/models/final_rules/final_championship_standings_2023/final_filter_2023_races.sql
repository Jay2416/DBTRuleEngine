{{ config(materialized='table') }}

{{ rule_engine(
    tables=['stg_results', 'stg_races'], joins=[{'type': 'INNER JOIN', 'table': 'stg_races', 'left': 'stg_results.race_id', 'right': 'stg_races.race_id'}], select_columns=['stg_results.driver_id', 'stg_results.points_scored'],
    aggregations=[], where_filters=[{'col': 'stg_races.race_year', 'op': '=', 'value': '2023', 'logic': 'AND'}], group_by=[],
    having=[], order_by=[], limit_rows=None
) }}