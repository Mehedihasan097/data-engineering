{% macro sum_aggregate(columns, aliases=none) %}
  {% set sum_list = [] %}
  {% for col in columns %}
    {% if aliases and aliases[loop.index0] is not none %}
      {% do sum_list.append("SUM(" ~ col ~ ") AS " ~ aliases[loop.index0]) %}
    {% else %}
      {% do sum_list.append("SUM(" ~ col ~ ") AS " ~ col) %}
    {% endif %}
  {% endfor %}
  {{ sum_list | join(', ') }}
{% endmacro %}
