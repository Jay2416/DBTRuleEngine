{% macro non_sequential_rule_engine(key_col, target_col, steps=[]) %}

WITH combined_flags AS (
{% for step in steps %}
    SELECT
        {{ step.key_col }} AS key_column,
        '{{ step.flag_value }}' AS {{ target_col }}

    {% if step.is_primary_cte %}
    FROM {{ step.primary_table }}
    {% else %}
    FROM {{ ref(step.primary_table) }}
    {% endif %}

    {% for j in step.joins %}
        {% if j.is_cte %}
        {{ j.type }} {{ j.table }} ON {{ j.left }} = {{ j.right }}
        {% else %}
        {{ j.type }} {{ ref(j.table) }} ON {{ j.left }} = {{ j.right }}
        {% endif %}
    {% endfor %}

    {% if step.where_filters|length > 0 %}
    WHERE
    {% for w in step.where_filters %}
        {{ w.col }} {{ w.op }} {{ w.value }}{% if not loop.last %} {{ w.logic }}{% endif %}
    {% endfor %}
    {% endif %}

    {% if not loop.last %}
    UNION ALL
    {% endif %}
{% endfor %}
)

SELECT
    key_column AS {{ key_col }},
    LISTAGG({{ target_col }}, ', ') AS {{ target_col }}_list
FROM combined_flags
GROUP BY key_column

{% endmacro %}
