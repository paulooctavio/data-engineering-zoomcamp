{{ config(materialized='table') }}

WITH yellow_data AS (
    SELECT
        *
    FROM {{ ref('stg_yellow_tripdata') }}
),

dim_zones AS (
    SELECT
        *
    FROM {{ ref('dim_zones') }}
    WHERE borough != 'Unknown'
)

SELECT
    y.trip_id
    ,y.vendorid
    ,y.ratecodeid
    ,y.pickup_datetime
    ,y.dropoff_datetime
    ,y.pickup_locationid
    ,pu.borough as pickup_borough
    ,pu.zone as pickup_zone
    ,pu.service_zone as pickup_service_zone
    ,y.dropoff_locationid
    ,do.borough as dropoff_borough
    ,do.zone as dropoff_zone
    ,do.service_zone as dropoff_service_zone
    ,y.passenger_count
    ,y.trip_distance
    ,y.fare_amount
    ,y.extra
    ,y.mta_tax
    ,y.tip_amount
    ,y.tolls_amount
    ,y.improvement_surcharge
    ,y.total_amount
    ,y.payment_type
    ,y.payment_type_description
    ,y.congestion_surcharge
FROM yellow_data y
INNER JOIN dim_zones pu
ON y.pickup_locationid = pu.locationid
INNER JOIN dim_zones do
ON y.dropoff_locationid = do.locationid