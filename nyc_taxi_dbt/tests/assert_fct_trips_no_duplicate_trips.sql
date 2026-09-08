{{ config(severity = 'warn') }}

-- fct_trips has no surrogate key: the incremental watermark appends anything with
-- a pickup_datetime past the previous max, so a re-run that overlaps an already
-- loaded window relies on full-row matching to avoid duplicates. That is fragile.
-- This test surfaces exact duplicates on the trip's identifying attributes.
-- Warns rather than errors: two genuinely distinct trips can share all of these
-- values, so a small count is plausible, but a sudden jump means a double load.
select
    vendor_id,
    pickup_datetime,
    dropoff_datetime,
    pickup_location_id,
    dropoff_location_id,
    total_amount,
    count(*) as duplicate_count
from {{ ref('fct_trips') }}
group by 1, 2, 3, 4, 5, 6
having count(*) > 1
