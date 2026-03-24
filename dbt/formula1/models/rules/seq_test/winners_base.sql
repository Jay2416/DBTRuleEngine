{{ config(materialized='table') }}

{{ rule_engine(
    tables=['stg_results', 'stg_races'], joins=[{'type': 'INNER JOIN', 'table': 'stg_races', 'left': 'stg_results.race_id', 'right': 'stg_races.race_id'}], select_columns=['stg_results.race_id', 'stg_results.driver_id', 'stg_results.points_scored', 'stg_results.finishing_position'],
    aggregations=[], where_filters=[{'col': 'stg_results.finishing_position', 'op': '=', 'value': '1', 'value2': '', 'logic': 'AND'}], group_by=[],
    having=[], order_by=[], limit_rows=None
) }}