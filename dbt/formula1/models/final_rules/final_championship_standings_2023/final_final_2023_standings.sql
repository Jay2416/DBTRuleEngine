{{ config(materialized='table') }}

{{ rule_engine(
    tables=['final_aggregate_driver_points', 'stg_drivers'], joins=[{'type': 'LEFT JOIN', 'table': 'stg_drivers', 'left': 'final_aggregate_driver_points.driver_id', 'right': 'stg_drivers.driver_id'}], select_columns=['final_aggregate_driver_points.total_points', 'stg_drivers.first_name', 'stg_drivers.last_name', 'stg_drivers.nationality'],
    aggregations=[], where_filters=[], group_by=[],
    having=[], order_by=[{'col': 'final_aggregate_driver_points.total_points', 'dir': 'DESC'}], limit_rows=None
) }}