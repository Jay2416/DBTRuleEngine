WITH stg_drivers AS (
    SELECT * FROM {{ ref('stg_drivers') }}
)

SELECT
    stg_drivers.driver_id,
    stg_drivers.driver_number,
    stg_drivers.date_of_birth
FROM stg_drivers
WHERE stg_drivers.last_name IS NOT NULL
ORDER BY stg_drivers.date_of_birth DESC
LIMIT 32