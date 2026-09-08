with trips as (
    select * from {{ ref('fct_trips') }}
),

zones as (
    select * from {{ ref('taxi_zone_lookup') }}
)

select
    zones.zone               as pickup_zone,
    zones.borough            as borough,
    count(*)                 as total_trips,
    sum(trips.total_amount)  as total_revenue,
    avg(trips.total_amount)  as avg_fare
from trips
left join zones
    on trips.pickup_location_id = zones.locationid
group by 1, 2
order by total_revenue desc