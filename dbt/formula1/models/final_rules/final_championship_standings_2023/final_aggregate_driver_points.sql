{{ config(materialized='table') }}

{{ rule_engine(
    tables=['final_filter_2023_races'], joins=[], select_columns=['final_filter_2023_races.driver_id'],
    aggregations=[{'func': 'SUM', 'col': 'final_filter_2023_races.points_scored', 'alias': 'total_points'}], where_filters=[], group_by=['final_filter_2023_races.driver_id'],
    having=[], order_by=[], limit_rows=None
) }}