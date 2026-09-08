{{ config(severity = 'warn') }}

-- TLC data contains a small number of trips whose dropoff precedes pickup, which
-- makes any duration calculation negative. Not filtered in fct_trips today, so
-- this warns to keep the count visible rather than failing the build.
select
    vendor_id,
    pickup_datetime,
    dropoff_datetime
from {{ ref('fct_trips') }}
where dropoff_datetime < pickup_datetime
