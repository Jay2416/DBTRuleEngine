{{ config(materialized='ephemeral') }}

{{ rule_engine(
    tables=['stg_races'],
    joins=[],
    select_columns=['stg_races.race_id'],
    aggregations=[],
    where_filters=[{'col': 'stg_races.race_year', 'op': '=', 'value': '2023', 'logic': 'AND'}],
    group_by=[],
    having=[],
    order_by=[],
    limit_rows=None
) }}