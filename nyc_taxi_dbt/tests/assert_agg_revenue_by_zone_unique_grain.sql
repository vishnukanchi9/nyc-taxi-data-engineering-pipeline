-- agg_revenue_by_zone claims a grain of one row per (pickup_zone, borough).
-- Errors if the seed ever introduces a duplicate zone name within a borough,
-- which would silently split that zone's revenue across two rows.
select
    pickup_zone,
    borough,
    count(*) as row_count
from {{ ref('agg_revenue_by_zone') }}
group by 1, 2
having count(*) > 1
