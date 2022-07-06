{{ config(materialized='view') }}

SELECT 
    -- identifiers
    dispatching_base_num
    ,Affiliated_base_number
	,cast(PUlocationID as integer) as PUlocationID
	,cast(DOlocationID as integer) as DOlocationID

    -- timestamp
	,cast(pickup_datetime as timestamp) as pickup_datetime
	,cast(dropOff_datetime as timestamp) as dropOff_datetime
     
     -- others
    ,cast(SR_Flag as integer) as SR_Flag

FROM {{ source('staging','fhv_tripdata_external_table') }}

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

    LIMIT 100

{% endif %}
