with source as (
    select * from {{ source('raw', 'yellow_tripdata') }}
),

renamed as (
    select
        "VendorID"                                          as vendor_id,
        to_timestamp_ntz("tpep_pickup_datetime", 6)          as pickup_datetime,
        to_timestamp_ntz("tpep_dropoff_datetime", 6)         as dropoff_datetime,
        "passenger_count"                                    as passenger_count,
        "trip_distance"                                      as trip_distance,
        "RatecodeID"                                          as rate_code_id,
        "store_and_fwd_flag"                                  as store_and_fwd_flag,
        "PULocationID"                                        as pickup_location_id,
        "DOLocationID"                                        as dropoff_location_id,
        "payment_type"                                        as payment_type,
        "fare_amount"                                         as fare_amount,
        "extra"                                               as extra_charges,
        "mta_tax"                                             as mta_tax,
        "tip_amount"                                          as tip_amount,
        "tolls_amount"                                        as tolls_amount,
        "improvement_surcharge"                               as improvement_surcharge,
        "total_amount"                                        as total_amount,
        "congestion_surcharge"                                as congestion_surcharge
    from source
)

select * from renamed