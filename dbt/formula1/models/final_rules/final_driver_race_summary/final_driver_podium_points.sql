{{ config(materialized='table') }}

{{ rule_engine(
    tables=['podium_finishers'], joins=[], select_columns=['podium_finishers.driver_id', 'podium_finishers.last_name'],
    aggregations=[{'func': 'SUM', 'col': 'podium_finishers.points_scored', 'alias': 'total_points'}], where_filters=[], group_by=['podium_finishers.driver_id', 'podium_finishers.last_name'],
    having=[], order_by=[], limit_rows=None
) }}