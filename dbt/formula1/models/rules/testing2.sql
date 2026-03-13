WITH stg_drivers AS (
    SELECT * FROM {{ ref('stg_drivers') }}
),
stg_results AS (
    SELECT * FROM {{ ref('stg_results') }}
)

SELECT
    stg_drivers.first_name,
    stg_drivers.last_name,
    stg_drivers.date_of_birth,
    stg_results.car_number,
    stg_results.finishing_position
FROM stg_drivers
INNER JOIN stg_results
  ON stg_drivers.driver_id = stg_results.driver_id
WHERE stg_drivers.driver_code IS NOT NULL
GROUP BY stg_drivers.first_name, stg_drivers.last_name, stg_drivers.date_of_birth, stg_results.car_number, stg_results.finishing_position
HAVING AVG(stg_results.laps_completed) > 10
ORDER BY stg_drivers.date_of_birth DESC