{{ config(materialized='table') }}

{{ rule_engine(
    tables=['base_driver_results'], joins=[], select_columns=['base_driver_results.driver_id', 'base_driver_results.points_scored', 'base_driver_results.last_name', 'base_driver_results.race_id'],
    aggregations=[], where_filters=[{'col': 'base_driver_results.finishing_position', 'op': '<=', 'value': '3', 'value2': '', 'logic': 'AND'}], group_by=[],
    having=[], order_by=[], limit_rows=None
) }}