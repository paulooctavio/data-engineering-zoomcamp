-- Question 1
SELECT 
  COUNT(*) AS COUNT
FROM 
  `dtc-de-352419.trips_data_all.fhv_tripdata_partitioned` 
WHERE 
  EXTRACT(YEAR FROM DATE(pickup_datetime)) = 2019;
-- Answer: 43261276

-- Question 2
SELECT 
  COUNT(DISTINCT dispatching_base_num) AS COUNT
FROM 
  `dtc-de-352419.trips_data_all.fhv_tripdata_partitioned` 
WHERE 
  EXTRACT(YEAR FROM DATE(pickup_datetime)) = 2019;
-- Answer: 799

-- Question 3
-- Answer:Partition by dropoff_datetime and cluster by dispatching_base_num

-- Question 4
CREATE OR REPLACE TABLE `dtc-de-352419.trips_data_all.fhv_tripdata_partitioned_clustered`
PARTITION BY
  DATE(pickup_datetime)    
CLUSTER BY
  dispatching_base_num AS
SELECT
 * EXCEPT (PUlocationID, DOlocationID, SR_Flag)
FROM 
  `dtc-de-352419.trips_data_all.fhv_tripdata_external_table`;

SELECT
  *
FROM 
  `dtc-de-352419.trips_data_all.fhv_tripdata_partitioned_clustered`
WHERE
  pickup_datetime BETWEEN '2019-01-01' AND '2019-03-31'
  AND dispatching_base_num IN ('B00987','B02060','B02279');
-- Answer: Estimated -> 790 MB, Actual -> 291.49 MB

-- Question 5
-- Answer: Cluster by dispatching_base_num and SR_Flag

-- Question 6
-- Answer: No improvement

-- Question 7
-- Answer: Columnnar
