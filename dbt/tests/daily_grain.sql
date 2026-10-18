select source_month, flight_date, airline, origin, destination
from {{ ref('fct_daily_operations') }}
group by all having count(*) > 1
