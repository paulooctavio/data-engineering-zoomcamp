{{ config(materialized='view') }}

with tripdata AS 
(
    SELECT
        *
        ,row_number() OVER(PARTITION BY vendorid, tpep_pickup_datetime) AS rn
    FROM {{ source('staging','yellow_tripdata_external_table') }}
    WHERE vendorid IS NOT NULL
)

SELECT
   -- identifiers
    {{ dbt_utils.surrogate_key(['vendorid', 'tpep_pickup_datetime'])}} AS trip_id
    ,cast(vendorid AS integer) AS vendorid
    ,cast(ratecodeid AS integer) AS ratecodeid
    ,cast(pulocationid AS integer) AS pickup_locationid
    ,cast(dolocationid AS integer) AS dropoff_locationid
    
    -- timestamps
    ,cast(tpep_pickup_datetime AS timestamp) AS pickup_datetime
    ,cast(tpep_dropoff_datetime AS timestamp) AS dropoff_datetime
    
    -- trip info
    ,store_and_fwd_flag
    ,cast(passenger_count AS integer) AS passenger_count
    ,cast(trip_distance AS numeric) AS trip_distance
    -- yellow cabs are always street-hail
    ,1 as trip_type
    
    -- payment info
    ,cast(fare_amount AS numeric) AS fare_amount
    ,cast(extra AS numeric) AS extra
    ,cast(mta_tax AS numeric) AS mta_tax
    ,cast(tip_amount AS numeric) AS tip_amount
    ,cast(tolls_amount AS numeric) AS tolls_amount
    ,cast(0 as numeric) as ehail_fee
    ,cast(improvement_surcharge AS numeric) AS improvement_surcharge
    ,cast(total_amount AS numeric) AS total_amount
    ,cast(payment_type AS integer) AS payment_type
    ,{{ get_payment_type_description('payment_type') }} AS payment_type_description
    ,cast(congestion_surcharge AS numeric) AS congestion_surcharge
FROM tripdata
WHERE rn = 1

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

    LIMIT 100

{% endif %}