{{ config(materialized='incremental') }}

with staging as (
    select * from {{ ref('stg_yellow_tripdata') }}
)

select *
from staging
where total_amount >= 0
  and pickup_datetime >= '2020-01-01'
  and pickup_datetime <= current_timestamp()
{% if is_incremental() %}
  and pickup_datetime > (select coalesce(max(pickup_datetime), '1900-01-01'::timestamp) from {{ this }})
{% endif %}