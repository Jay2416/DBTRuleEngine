WITH stg_drivers AS (
    SELECT * FROM {{ ref('stg_drivers') }}
),
stg_results AS (
    SELECT * FROM {{ ref('stg_results') }}
),
stg_races AS (
    SELECT * FROM {{ ref('stg_races') }}
)

SELECT
    stg_drivers.first_name,
    stg_drivers.last_name,
    stg_drivers.driver_number,
    stg_results.finishing_position,
    stg_results.points_scored,
    stg_races.race_name,
    stg_races.race_year
FROM stg_drivers
RIGHT JOIN stg_results
  ON stg_drivers.driver_id = stg_results.driver_id
INNER JOIN stg_races
  ON stg_results.race_id = stg_races.race_id