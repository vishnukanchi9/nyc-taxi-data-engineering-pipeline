-- Asserts that the negative-fare filter in fct_trips actually did its job.
-- The companion test assert_no_negative_fares warns that these rows exist in
-- staging (a known source condition); this one errors if any survived into the
-- mart, which would mean the filter regressed.
select
    vendor_id,
    pickup_datetime,
    total_amount
from {{ ref('fct_trips') }}
where total_amount < 0
