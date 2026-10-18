select * from {{ ref('fct_daily_operations') }}
where cancelled > flights or operated > flights
   or departure_on_time > departure_observed or departure_observed > operated
   or arrival_on_time > arrival_observed or arrival_observed > operated
