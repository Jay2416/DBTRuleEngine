{{ config(materialized='table') }}

{{ rule_engine(
    tables=['stg_circuits'], joins=[], select_columns=['stg_circuits.circuit_id', 'stg_circuits.circuit_ref', 'stg_circuits.circuit_name'],
    aggregations=[], where_filters=[{'col': 'stg_results.position_order', 'op': '=', 'value': '0', 'logic': 'AND'}], group_by=[],
    having=[], order_by=[], limit_rows=None
) }}