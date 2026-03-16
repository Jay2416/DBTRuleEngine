{{ rule_engine(

tables=['stg_drivers','stg_results'],

joins=[
    {
        "type":"INNER JOIN",
        "table":"stg_results",
        "left":"stg_drivers.driver_id",
        "right":"stg_results.driver_id"
    }
],

select_columns=[
    "stg_drivers.nationality",
    "stg_drivers.last_name",
    "stg_results.race_id",
    "stg_results.car_number",
    "stg_results.points_scored",
    "stg_drivers.date_of_birth"
],

aggregations=[],

where_filters=[
    {
        "col":"stg_drivers.driver_code",
        "op":"IS NOT",
        "value":"NULL",
        "logic":"AND"
    }
],

group_by=[
"stg_drivers.nationality",
"stg_drivers.last_name",
"stg_results.race_id",
"stg_results.car_number",
"stg_results.points_scored",
"stg_drivers.date_of_birth"
],

having=[
    {
        "func":"COUNT",
        "col":"stg_drivers.first_name",
        "op":">",
        "value":"200",
        "logic":"AND"
    }
],

order_by=[
    {
        "col":"stg_drivers.date_of_birth",
        "dir":"DESC"
    }
],

limit_rows=None

) }}