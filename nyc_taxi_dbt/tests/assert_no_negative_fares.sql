{{ config(severity = 'warn') }}

-- Known data quality issue: ~1.2% of raw trips have negative total_amount
-- (refunds/corrections). Intentionally left unfiltered in staging;
-- excluded downstream in fct_trips. This test warns rather than errors
-- since it documents a known, handled condition.
select
    vendor_id,
    pickup_datetime,
    total_amount
from {{ ref('stg_yellow_tripdata') }}
where total_amount < 0