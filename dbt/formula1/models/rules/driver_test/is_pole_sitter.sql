{{ config(materialized='ephemeral') }}

{{ rule_engine(
    tables=['stg_qualifying'], joins=[], select_columns=['stg_qualifying.driver_id', "'Started On Pole' AS remark"],
    aggregations=[], where_filters=[{'col': 'stg_qualifying.qualifying_position', 'op': '=', 'value': '1', 'value2': '', 'logic': 'AND'}], group_by=[],
    having=[], order_by=[], limit_rows=None
) }}