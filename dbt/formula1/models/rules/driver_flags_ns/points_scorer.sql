{{ config(materialized='table') }}

{{ rule_engine(
    tables=['stg_results'], joins=[], select_columns=['stg_results.driver_id', 'stg_results.points_scored'],
    aggregations=[], where_filters=[{'col': 'stg_results.points_scored', 'op': 'greater than (>)', 'value': '0', 'logic': 'AND'}], group_by=[],
    having=[], order_by=[], limit_rows=None
) }}