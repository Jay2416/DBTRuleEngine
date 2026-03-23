{{ config(materialized='table') }}

{{ rule_engine(
    tables=['stg_results'], joins=[], select_columns=['stg_results.driver_id', 'stg_results.fastest_lap_rank'],
    aggregations=[], where_filters=[{'col': 'stg_results.fastest_lap_rank', 'op': 'less than equal (<=)', 'value': '3', 'logic': 'AND'}], group_by=[],
    having=[], order_by=[], limit_rows=None
) }}