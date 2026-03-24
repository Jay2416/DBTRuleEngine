{{ config(materialized='table') }}

{{ rule_engine(
    tables=['winners_base'], joins=[], select_columns=['winners_base.driver_id', 'winners_base.race_id', 'winners_base.points_scored'],
    aggregations=[], where_filters=[{'col': 'winners_base.points_scored', 'op': '>=', 'value': '24', 'value2': '', 'logic': 'AND'}], group_by=[],
    having=[], order_by=[], limit_rows=None
) }}