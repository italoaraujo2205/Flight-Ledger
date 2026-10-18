select *,
    status = 'CANCELADO' as is_cancelled,
    status = 'REALIZADO' as is_operated,
    status = 'REALIZADO' and scheduled_departure_utc is not null
        and actual_departure_utc is not null as departure_eligible,
    status = 'REALIZADO' and scheduled_arrival_utc is not null
        and actual_arrival_utc is not null as arrival_eligible
from {{ source('raw', 'flights') }}
