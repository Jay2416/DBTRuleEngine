WITH stg_drivers AS (
    SELECT * FROM {{ ref('stg_drivers') }}
),
stg_results AS (
    SELECT * FROM {{ ref('stg_results') }}
)

SELECT
    stg_drivers.nationality,
    stg_drivers.last_name,
    stg_results.race_id,
    stg_results.car_number,
    stg_results.points_scored,
    stg_drivers.date_of_birth
FROM stg_drivers
INNER JOIN stg_results
  ON stg_drivers.driver_id = stg_results.driver_id
WHERE stg_drivers.driver_code IS NOT NULL
GROUP BY stg_drivers.nationality, stg_drivers.last_name, stg_results.race_id, stg_results.car_number, stg_results.points_scored
HAVING COUNT(stg_drivers.first_name) > 200
ORDER BY stg_drivers.date_of_birth DESC