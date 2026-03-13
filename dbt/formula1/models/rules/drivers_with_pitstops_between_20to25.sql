WITH stg_pit_stops AS (
    SELECT * FROM {{ ref('stg_pit_stops') }}
),
stg_drivers AS (
    SELECT * FROM {{ ref('stg_drivers') }}
)

SELECT
    stg_pit_stops.pit_stop_duration_ms,
    stg_drivers.first_name,
    stg_drivers.last_name,
    stg_drivers.driver_number
FROM stg_pit_stops
FULL OUTER JOIN stg_drivers ON stg_pit_stops.driver_id = stg_drivers.driver_id
WHERE stg_pit_stops.pit_stop_duration_ms > 20000
  AND stg_pit_stops.pit_stop_duration_ms < 25000