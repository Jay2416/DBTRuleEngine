{% macro non_sequential_rule_engine(rule_models, key_column, remark_column='remark', target_column=None) %}

{% if rule_models | length == 0 %}
    {% do exceptions.raise_compiler_error("non_sequential_rule_engine requires at least one model") %}
{% endif %}

{% set output_remark_column = remark_column if remark_column else target_column %}

{% if not output_remark_column %}
    {% do exceptions.raise_compiler_error("non_sequential_rule_engine requires remark_column (or target_column for backward compatibility)") %}
{% endif %}

WITH stacked_rules AS (
    {% for model in rule_models %}
        {% set model_name = model %}
        {% if model_name is string and model_name.startswith('final_') %}
            {% set model_name = model_name[6:] %}
        {% endif %}
        SELECT
            {{ adapter.quote(key_column) }} AS rule_key,
            CAST({{ adapter.quote(output_remark_column) }} AS STRING) AS rule_value
        FROM {{ ref(model_name) }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
    {% endfor %}
),
deduped_rules AS (
    SELECT DISTINCT rule_key, rule_value
    FROM stacked_rules
)

SELECT
    rule_key AS {{ adapter.quote(key_column) }},
    -- Spark 4.0 LISTAGG: combines all rule outputs per key with ", " delimiter.
    listagg(rule_value, ', ') WITHIN GROUP (ORDER BY rule_value) AS {{ adapter.quote(output_remark_column) }}
FROM deduped_rules
WHERE rule_key IS NOT NULL
    AND rule_value IS NOT NULL
GROUP BY rule_key

{% endmacro %}