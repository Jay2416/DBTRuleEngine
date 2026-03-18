{{ config(materialized='table') }}

{{ rule_engine(
    tables=['stg_circuits'],
    joins=[],
    select_columns=['stg_circuits.circuit_id', 'stg_circuits.circuit_name'],
    aggregations=[],
    where_filters=[],
    group_by=[],
    having=[],
    order_by=[],
    limit_rows=None
) }}