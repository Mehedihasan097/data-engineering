{% macro group_by(n) %}
  {% set group_by_list = [] %}
  {% for i in range(1, n + 1) %}
    {% do group_by_list.append(i|string) %}
  {% endfor %}
  {% if group_by_list %}
    GROUP BY {{ group_by_list | join(', ') }}
  {% endif %}
{% endmacro %}
    