select
    date_trunc('day', pickup_datetime)   as trip_date,
    count(*)                             as total_trips,
    sum(total_amount)                    as total_revenue,
    avg(trip_distance)                   as avg_trip_distance,
    avg(total_amount)                    as avg_fare
from {{ ref('fct_trips') }}
group by 1
order by 1