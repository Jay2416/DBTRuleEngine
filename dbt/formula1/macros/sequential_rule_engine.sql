{% macro sequential_rule_engine(steps=[]) %}

WITH

{% set cte_names = steps | map(attribute='rule_name') | list %}

{% for step in steps %}
{{ step.rule_name }} AS (
    {% set primary_ns = namespace(name=step.primary_table, is_cte=step.is_primary_cte or ('{{' in (step.primary_table|string))) %}
    {% for cte in cte_names %}
        {% if primary_ns.name == ('final_' ~ cte) %}
            {% set primary_ns.name = cte %}
            {% set primary_ns.is_cte = true %}
        {% endif %}
    {% endfor %}

    SELECT
    {% for col in step.select_columns %}
        {% set col_ns = namespace(value=col) %}
        {% for cte in cte_names %}
            {% set col_ns.value = col_ns.value | replace('final_' ~ cte ~ '.', cte ~ '.') %}
        {% endfor %}
        {{ col_ns.value }}{% if not loop.last or step.aggregations|length > 0 %},{% endif %}
    {% endfor %}

    {% for agg in step.aggregations %}
        {% set agg_col_ns = namespace(value=agg.col) %}
        {% for cte in cte_names %}
            {% set agg_col_ns.value = agg_col_ns.value | replace('final_' ~ cte ~ '.', cte ~ '.') %}
        {% endfor %}
        {{ agg.func }}({{ agg_col_ns.value }}) AS {{ agg.alias }}{% if not loop.last %},{% endif %}
    {% endfor %}

    -- Use raw name if it's an internal CTE or already a templated relation; otherwise, wrap in ref().
    {% if primary_ns.is_cte %}
    FROM {{ primary_ns.name }}
    {% else %}
    FROM {{ ref(primary_ns.name) }}
    {% endif %}

    {% for j in step.joins %}
        {% set join_ns = namespace(table=j.table, is_cte=j.is_cte or ('{{' in (j.table|string)), left=j.left, right=j.right) %}
        {% for cte in cte_names %}
            {% if join_ns.table == ('final_' ~ cte) %}
                {% set join_ns.table = cte %}
                {% set join_ns.is_cte = true %}
            {% endif %}
            {% set join_ns.left = join_ns.left | replace('final_' ~ cte ~ '.', cte ~ '.') %}
            {% set join_ns.right = join_ns.right | replace('final_' ~ cte ~ '.', cte ~ '.') %}
        {% endfor %}
        {% if join_ns.is_cte %}
        {{ j.type }} {{ join_ns.table }} ON {{ join_ns.left }} = {{ join_ns.right }}
        {% else %}
        {{ j.type }} {{ ref(join_ns.table) }} ON {{ join_ns.left }} = {{ join_ns.right }}
        {% endif %}
    {% endfor %}

    {% if step.where_filters|length > 0 %}
    WHERE
    {% for w in step.where_filters %}
        {% set where_col_ns = namespace(value=w.col) %}
        {% for cte in cte_names %}
            {% set where_col_ns.value = where_col_ns.value | replace('final_' ~ cte ~ '.', cte ~ '.') %}
        {% endfor %}
        {{ where_col_ns.value }} {{ w.op }} {{ w.value }} {% if not loop.last %} {{ w.logic }} {% endif %}
    {% endfor %}
    {% endif %}

    {% if step.group_by|length > 0 %}
    GROUP BY
    {% for g in step.group_by %}
        {% set group_col_ns = namespace(value=g) %}
        {% for cte in cte_names %}
            {% set group_col_ns.value = group_col_ns.value | replace('final_' ~ cte ~ '.', cte ~ '.') %}
        {% endfor %}
        {{ group_col_ns.value }}{% if not loop.last %},{% endif %}
    {% endfor %}
    {% endif %}

    {% if step.having|length > 0 %}
    HAVING
    {% for h in step.having %}
        {% set having_col_ns = namespace(value=h.col) %}
        {% for cte in cte_names %}
            {% set having_col_ns.value = having_col_ns.value | replace('final_' ~ cte ~ '.', cte ~ '.') %}
        {% endfor %}
        {{ h.func }}({{ having_col_ns.value }}) {{ h.op }} {{ h.value }} {% if not loop.last %} {{ h.logic }} {% endif %}
    {% endfor %}
    {% endif %}

    {% if step.order_by|length > 0 %}
    ORDER BY
    {% for o in step.order_by %}
        {% set order_col_ns = namespace(value=o.col) %}
        {% for cte in cte_names %}
            {% set order_col_ns.value = order_col_ns.value | replace('final_' ~ cte ~ '.', cte ~ '.') %}
        {% endfor %}
        {{ order_col_ns.value }} {{ o.dir }}{% if not loop.last %},{% endif %}
    {% endfor %}
    {% endif %}

    {% if step.limit_rows and step.limit_rows != 'None' %}
    LIMIT {{ step.limit_rows }}
    {% endif %}

){% if not loop.last %},{% endif %}

{% endfor %}

/* --- Final Execution --- */
SELECT * FROM {{ steps[-1].rule_name }}

{% endmacro %}