with routes as (
    select source_month, airline, origin, destination,
        sum(flights) as flights, sum(cancelled) as cancelled, sum(operated) as operated,
        sum(departure_observed) as departure_observed,
        sum(departure_on_time) as departure_on_time,
        sum(arrival_observed) as arrival_observed, sum(arrival_on_time) as arrival_on_time
    from {{ ref('fct_daily_operations') }}
    group by source_month, airline, origin, destination
)
select r.*, a.airport_name as origin_name, a.iso_region as origin_region,
    a.airport_code is null as origin_unmatched,
    1.0 * cancelled / nullif(flights, 0) as cancellation_rate,
    1.0 * departure_observed / nullif(operated, 0) as departure_coverage,
    1.0 * departure_on_time / nullif(departure_observed, 0) as departure_otp15,
    1.0 * arrival_on_time / nullif(arrival_observed, 0) as arrival_otp15
from routes r left join {{ ref('dim_airport') }} a on r.origin = a.airport_code
