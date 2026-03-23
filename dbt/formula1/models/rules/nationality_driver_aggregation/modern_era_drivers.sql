{{ config(materialized='table') }}

{{ rule_engine(
    tables=['stg_drivers'], joins=[], select_columns=['stg_drivers.driver_ref', 'stg_drivers.nationality'],
    aggregations=[], where_filters=[{'col': 'stg_drivers.driver_id', 'op': '>=', 'value': '20', 'value2': '', 'logic': 'AND'}], group_by=[],
    having=[], order_by=[], limit_rows=None
) }}