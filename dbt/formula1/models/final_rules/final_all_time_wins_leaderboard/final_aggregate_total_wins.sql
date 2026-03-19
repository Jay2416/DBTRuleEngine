{{ config(materialized='table') }}

{{ rule_engine(
    tables=['filter_first_place'], joins=[], select_columns=['filter_first_place.driver_id'],
    aggregations=[{'func': 'COUNT', 'col': 'filter_first_place.race_id', 'alias': 'total_wins'}], where_filters=[], group_by=['filter_first_place.driver_id'],
    having=[], order_by=[], limit_rows=None
) }}