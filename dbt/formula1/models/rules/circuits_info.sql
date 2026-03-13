SELECT stg_circuits.circuit_id,
    stg_circuits.circuit_ref,
    stg_circuits.circuit_name
FROM stg_circuits
WHERE stg_circuits.circuit_id > 4