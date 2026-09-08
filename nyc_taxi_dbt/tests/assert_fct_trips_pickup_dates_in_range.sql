-- A handful of raw trips per month carry corrupted pickup dates (stamped 2002,
-- 2009, or in the future). fct_trips bounds them out; this errors if any leaked
-- through, which would skew every downstream daily aggregate.
select
    vendor_id,
    pickup_datetime,
    total_amount
from {{ ref('fct_trips') }}
where pickup_datetime < '2020-01-01'
   or pickup_datetime > current_timestamp()
