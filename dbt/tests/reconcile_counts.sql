with raw_counts as (
    select source_month, count(*) as total from {{ ref('fct_flight') }} group by source_month
), daily_counts as (
    select source_month, sum(flights) as total from {{ ref('fct_daily_operations') }} group by source_month
)
select coalesce(r.source_month, d.source_month) as source_month
from raw_counts r full join daily_counts d using (source_month)
where r.total is distinct from d.total
