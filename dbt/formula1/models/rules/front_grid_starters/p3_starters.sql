{{ config(materialized='ephemeral') }}

{{ rule_engine(
    tables=['stg_results'], joins=[], select_columns=['stg_results.race_id', 'stg_results.driver_id'],
    aggregations=[], where_filters=[{'col': 'stg_results.grid_position', 'op': '=', 'value': '3', 'value2': '', 'logic': 'AND'}], group_by=[],
    having=[], order_by=[], limit_rows=None
) }}