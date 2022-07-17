{{ config(materialized='table') }}

WITH yellow_data AS (
    SELECT
        *
        ,'Yellow' AS service_type
    FROM {{ ref('stg_yellow_tripdata') }}
),

green_data AS (
    SELECT
        *
        ,'Green' AS service_type
    FROM {{ ref('stg_green_tripdata') }}
),

trips_unioned as (
    SELECT * FROM green_data
    UNION ALL
    SELECT * FROM yellow_data
), 

dim_zones AS (
    SELECT
        *
    FROM {{ ref('dim_zones') }}
    WHERE borough != 'Unknown'
)

SELECT
    t.trip_id
    ,t.vendorid
    ,t.service_type
    ,t.ratecodeid
    ,t.pickup_datetime
    ,t.dropoff_datetime
    ,t.pickup_locationid
    ,pu.borough as pickup_borough
    ,pu.zone as pickup_zone
    ,pu.service_zone as pickup_service_zone
    ,t.dropoff_locationid
    ,do.borough as dropoff_borough
    ,do.zone as dropoff_zone
    ,do.service_zone as dropoff_service_zone
    ,t.passenger_count
    ,t.trip_distance
    ,t.fare_amount
    ,t.extra
    ,t.mta_tax
    ,t.tip_amount
    ,t.tolls_amount
    ,t.improvement_surcharge
    ,t.total_amount
    ,t.payment_type
    ,t.payment_type_description
    ,t.congestion_surcharge
FROM trips_unioned t
INNER JOIN dim_zones pu
ON t.pickup_locationid = pu.locationid
INNER JOIN dim_zones do
ON t.dropoff_locationid = do.locationid