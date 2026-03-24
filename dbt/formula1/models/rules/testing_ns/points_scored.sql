{{ config(materialized='ephemeral') }}

{{ rule_engine(
    tables=['stg_results'], joins=[], select_columns=['stg_results.driver_id', "'scored_point' AS remark"],
    aggregations=[], where_filters=[{'col': 'stg_results.points_scored', 'op': '>', 'value': '0', 'value2': '', 'logic': 'AND'}], group_by=[],
    having=[], order_by=[], limit_rows=None
) }}