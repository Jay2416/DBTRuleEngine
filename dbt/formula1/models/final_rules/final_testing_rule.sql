WITH stg_drivers AS (
    SELECT * FROM {{ ref('stg_drivers') }}
),
stg_results AS (
    SELECT * FROM {{ ref('stg_results') }}
)

SELECT
    stg_drivers.driver_id,
    stg_drivers.driver_ref,
    stg_drivers.driver_number,
    stg_results.car_number,
    stg_results.finishing_position
FROM stg_drivers
INNER JOIN stg_results
  ON stg_drivers.driver_id = stg_results.driver_id
WHERE stg_drivers.driver_number IS NOT NULL
LIMIT 50