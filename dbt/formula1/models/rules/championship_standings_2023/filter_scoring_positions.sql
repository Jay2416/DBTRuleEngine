{{ config(materialized='table') }}

{{ rule_engine(
    tables=['final_filter_2023_races'], joins=[], select_columns=['final_filter_2023_races.driver_id', 'final_filter_2023_races.points_scored'],
    aggregations=[], where_filters=[{'col': 'final_filter_2023_races.points_scored', 'op': 'greater than (>)', 'value': '0', 'logic': 'AND'}], group_by=[],
    having=[], order_by=[], limit_rows=None
) }}