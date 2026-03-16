{% macro rule_engine(
    tables=[],
    joins=[],
    select_columns=[],
    aggregations=[],
    where_filters=[],
    group_by=[],
    having=[],
    order_by=[],
    limit_rows=None
) %}

WITH

{% for tbl in tables %}
{{ tbl }} AS (
    SELECT * FROM {{ ref(tbl) }}
){% if not loop.last %},{% endif %}
{% endfor %}

SELECT

{% for col in select_columns %}
    {{ col }}{% if not loop.last or aggregations|length > 0 %},{% endif %}
{% endfor %}

{% for agg in aggregations %}
    {{ agg.func }}({{ agg.col }}) AS {{ agg.alias }}{% if not loop.last %},{% endif %}
{% endfor %}

FROM {{ tables[0] }}

{% for j in joins %}
{{ j.type }} {{ j.table }}
ON {{ j.left }} = {{ j.right }}
{% endfor %}

{% if where_filters|length > 0 %}
WHERE
{% for w in where_filters %}
    {{ w.col }} {{ w.op }} {{ w.value }}
    {% if not loop.last %} {{ w.logic }} {% endif %}
{% endfor %}
{% endif %}

{% if group_by|length > 0 %}
GROUP BY
{% for g in group_by %}
    {{ g }}{% if not loop.last %},{% endif %}
{% endfor %}
{% endif %}

{% if having|length > 0 %}
HAVING
{% for h in having %}
    {{ h.func }}({{ h.col }}) {{ h.op }} {{ h.value }}
    {% if not loop.last %} {{ h.logic }} {% endif %}
{% endfor %}
{% endif %}

{% if order_by|length > 0 %}
ORDER BY
{% for o in order_by %}
    {{ o.col }} {{ o.dir }}{% if not loop.last %},{% endif %}
{% endfor %}
{% endif %}

{% if limit_rows %}
LIMIT {{ limit_rows }}
{% endif %}

{% endmacro %}