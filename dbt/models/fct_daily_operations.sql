{{ config(materialized='incremental', incremental_strategy='append',
          pre_hook="{{ delete_changed_months(this) }}") }}
select source_month, flight_date, airline, origin, destination,
    count(*) as flights,
    count(*) filter (where is_cancelled) as cancelled,
    count(*) filter (where is_operated) as operated,
    count(*) filter (where departure_eligible) as departure_observed,
    count(*) filter (where departure_eligible and departure_delay_min <= 15) as departure_on_time,
    count(*) filter (where arrival_eligible) as arrival_observed,
    count(*) filter (where arrival_eligible and arrival_delay_min <= 15) as arrival_on_time,
    sum(case when departure_eligible then departure_delay_min else 0 end) as departure_delay_sum
from {{ ref('fct_flight') }}
{% if is_incremental() %}
where source_month in (
  {% for month in var('changed_months', []) %}
    '{{ month }}'{% if not loop.last %},{% endif %}
  {% else %} null {% endfor %}
)
{% endif %}
group by source_month, flight_date, airline, origin, destination
