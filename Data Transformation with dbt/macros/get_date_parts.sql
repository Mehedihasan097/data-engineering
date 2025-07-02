{% macro get_date_parts(date_column, year_alias='year', quarter_alias='quarter', month_alias='month') %}
  EXTRACT(YEAR FROM {{ date_column }}) AS {{ year_alias }},
  EXTRACT(QUARTER FROM {{ date_column }}) AS {{ quarter_alias }},
  EXTRACT(MONTH FROM {{ date_column }}) AS {{ month_alias }}
{% endmacro %}
