{% macro rule_group_by_having(staging_model, select_column, agg_function, agg_column, group_column, limit_rows=50) %}

WITH {{ staging_model }} AS (
    SELECT * FROM {{ ref(staging_model) }}
)

SELECT
    {{ staging_model }}.{{ select_column }},
    {{ agg_function }}({{ staging_model }}.{{ agg_column }})
FROM {{ staging_model }}

GROUP BY {{ staging_model }}.{{ group_column }}

LIMIT {{ limit_rows }}

{% endmacro %}