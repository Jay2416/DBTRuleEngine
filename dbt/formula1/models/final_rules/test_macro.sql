{{ rule_engine(
    rule_id = 1,
    rule_name = 'test_rule',
    tables = ['results', 'drivers'],
    joins = [
        {
            'type': 'LEFT JOIN',
            'table': 'drivers',
            'left': 'results.driverId',
            'right': 'drivers.driverId'
        }
    ],
    select_columns = ['results.driverId', 'results.points', 'drivers.surname'],
    where_filters = [
        {
            'col': 'results.points',
            'op': '>',
            'value': 10,
            'logic': 'AND'
        }
    ],
    order_by = [{'col': 'results.points', 'dir': 'DESC'}],
    limit_rows = 100
) }}