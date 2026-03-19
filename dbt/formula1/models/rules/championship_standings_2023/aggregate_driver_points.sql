{{ config(materialized='table') }}

{{ rule_engine(
    tables=['final_filter_scoring_positions'], joins=[], select_columns=['final_filter_scoring_positions.driver_id'],
    aggregations=[{'func': 'SUM', 'col': 'final_filter_scoring_positions.points_scored', 'alias': 'total_points'}], where_filters=[], group_by=['final_filter_scoring_positions.driver_id'],
    having=[], order_by=[], limit_rows=None
) }}