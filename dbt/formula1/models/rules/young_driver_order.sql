WITH stg_drivers AS (
    SELECT * FROM {{ ref('stg_drivers') }}
)

SELECT
    stg_drivers.first_name,
    stg_drivers.last_name,
    stg_drivers.date_of_birth
FROM stg_drivers
ORDER BY stg_drivers.date_of_birth DESC