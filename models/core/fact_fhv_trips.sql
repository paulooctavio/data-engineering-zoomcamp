{{ config(materialized='table') }}

WITH dim_zones AS (
    SELECT
        *
    FROM {{ ref('dim_zones') }}
    WHERE borough != 'Unknown'
)

SELECT
    f.dispatching_base_num
    ,f.Affiliated_base_number
    ,f.pickup_datetime
    ,f.dropOff_datetime
    ,f.SR_Flag
    ,f.pickup_locationid
    ,pu.borough as pickup_borough
    ,pu.zone as pickup_zone
    ,pu.service_zone as pickup_service_zone
    ,f.dropoff_locationid
    ,do.borough as dropoff_borough
    ,do.zone as dropoff_zone
    ,do.service_zone as dropoff_service_zone
FROM {{ ref('stg_fhv_tripdata') }} f
INNER JOIN dim_zones pu
ON f.pickup_locationid = pu.locationid
INNER JOIN dim_zones do
ON f.dropoff_locationid = do.locationid