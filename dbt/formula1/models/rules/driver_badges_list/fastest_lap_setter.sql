{{ config(materialized='table') }}

{{ rule_engine(
    tables=['stg_results'], joins=[], select_columns=['stg_results.driver_id'],
    aggregations=[], where_filters=[{'col': 'stg_results.finishing_position', 'op': '=', 'value': '1', 'logic': 'AND'}], group_by=[],
    having=[], order_by=[], limit_rows=None
) }}