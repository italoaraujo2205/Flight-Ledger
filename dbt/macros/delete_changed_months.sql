{% macro delete_changed_months(relation) %}
  {% if is_incremental() and var('changed_months', []) | length > 0 %}
    delete from {{ relation }} where source_month in (
      {% for month in var('changed_months') %}
        '{{ month }}'{% if not loop.last %},{% endif %}
      {% endfor %}
    )
  {% else %}
    select 1
  {% endif %}
{% endmacro %}
