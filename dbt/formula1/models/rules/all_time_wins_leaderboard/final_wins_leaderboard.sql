{{ config(materialized='table') }}

{{ rule_engine(
    tables=['aggregate_total_wins', 'stg_drivers'], joins=[{'type': 'LEFT JOIN', 'table': 'stg_drivers', 'left': 'aggregate_total_wins.driver_id', 'right': 'stg_drivers.driver_id'}], select_columns=['aggregate_total_wins.driver_id', 'aggregate_total_wins.total_wins', 'stg_drivers.first_name', 'stg_drivers.last_name'],
    aggregations=[], where_filters=[], group_by=[],
    having=[], order_by=[{'col': 'aggregate_total_wins.total_wins', 'dir': 'DESC'}], limit_rows=10
) }}