{% macro non_sequential_rule_engine(rule_models, key_column, target_column) %}

{% if rule_models | length == 0 %}
    {% do exceptions.raise_compiler_error("non_sequential_rule_engine requires at least one model") %}
{% endif %}

WITH stacked_rules AS (
    {% for model in rule_models %}
        {% set model_name = model %}
        {% if model_name is string and model_name.startswith('final_') %}
            {% set model_name = model_name[6:] %}
        {% endif %}
        SELECT
            {{ adapter.quote(key_column) }} AS rule_key,
            CAST({{ adapter.quote(target_column) }} AS STRING) AS rule_value
        FROM {{ ref(model_name) }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
    {% endfor %}
)

SELECT
    rule_key AS {{ adapter.quote(key_column) }},
    -- Spark 4.0 LISTAGG: combines all rule outputs per key with ", " delimiter.
    listagg(rule_value, ', ') WITHIN GROUP (ORDER BY rule_value) AS {{ adapter.quote(target_column) }}
FROM stacked_rules
WHERE rule_value IS NOT NULL
GROUP BY rule_key

{% endmacro %}