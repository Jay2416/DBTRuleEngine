WITH stg_results AS (
    SELECT * FROM {{ ref('stg_results') }}
)

SELECT
    MIN(stg_results.race_time_ms)
FROM stg_results
GROUP BY stg_results.driver_id
LIMIT 50