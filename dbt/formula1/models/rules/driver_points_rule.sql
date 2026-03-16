{{ rule_group_by_having(
    'stg_results',
    'race_id',
    'MIN',
    'race_time_ms',
    'race_id',
    50
) }}