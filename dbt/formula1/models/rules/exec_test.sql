WITH stg_circuits AS (
    SELECT * FROM {{ ref('stg_circuits') }}
)

SELECT
    stg_circuits.circuit_id,
    stg_circuits.circuit_ref,
    stg_circuits.circuit_name
FROM stg_circuits
LIMIT 40