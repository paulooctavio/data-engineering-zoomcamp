-- Create external table from parquet files
CREATE OR REPLACE EXTERNAL TABLE `dtc-de-352419.nytaxi.yellow_tripdata`
OPTIONS (
  format = 'parquet',
  uris = [
    'gs://dtc_data_lake_dtc-de-352419/raw/yellow_tripdata/2019/*',
    'gs://dtc_data_lake_dtc-de-352419/raw/yellow_tripdata/2020/*'
  ]
);

-- Create non partitoned table from external table
CREATE OR REPLACE TABLE `dtc-de-352419.nytaxi.yellow_tripdata_non_partitioned` AS
SELECT
  VendorID
  ,tpep_pickup_datetime
  ,tpep_dropoff_datetime
  ,passenger_count
  ,trip_distance
  ,RatecodeID
  ,store_and_fwd_flag
  ,PULocationID
  ,DOLocationID
  ,payment_type
  ,fare_amount
  ,extra
  ,mta_tax
  ,tip_amount
  ,tolls_amount
  ,improvement_surcharge
  ,total_amount
  ,congestion_surcharge
FROM
  `dtc-de-352419.nytaxi.yellow_tripdata`;

-- Create partitoned table from external table
CREATE OR REPLACE TABLE `dtc-de-352419.nytaxi.yellow_tripdata_partitioned`
PARTITION BY
  DATE(tpep_pickup_datetime) AS
SELECT
  VendorID
  ,tpep_pickup_datetime
  ,tpep_dropoff_datetime
  ,passenger_count
  ,trip_distance
  ,RatecodeID
  ,store_and_fwd_flag
  ,PULocationID
  ,DOLocationID
  ,payment_type
  ,fare_amount
  ,extra
  ,mta_tax
  ,tip_amount
  ,tolls_amount
  ,improvement_surcharge
  ,total_amount
  ,congestion_surcharge
FROM
  `dtc-de-352419.nytaxi.yellow_tripdata`;

-- Impact of partition
-- Scanning 1.6GB of data
SELECT
  DISTINCT(VendorID)
FROM 
  `dtc-de-352419.nytaxi.yellow_tripdata_non_partitioned`
WHERE
  DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30';

-- Scanning ~106MB of data
SELECT
  DISTINCT(VendorID)
FROM 
  `dtc-de-352419.nytaxi.yellow_tripdata_partitioned`
WHERE
  DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30';

-- Let's look into the partitions
SELECT
  table_name, partition_id, total_rows
FROM 
  `dtc-de-352419.nytaxi.INFORMATION_SCHEMA.PARTITIONS`
WHERE
  table_name = 'yellow_tripdata_partitioned'
ORDER BY
  total_rows DESC;

-- Creating a partitioned and clustered table
CREATE OR REPLACE TABLE `dtc-de-352419.nytaxi.yellow_tripdata_partitioned_clustered`
PARTITION BY
  DATE(tpep_pickup_datetime) 
CLUSTER BY
  VendorID AS
SELECT
  VendorID
  ,tpep_pickup_datetime
  ,tpep_dropoff_datetime
  ,passenger_count
  ,trip_distance
  ,RatecodeID
  ,store_and_fwd_flag
  ,PULocationID
  ,DOLocationID
  ,payment_type
  ,fare_amount
  ,extra
  ,mta_tax
  ,tip_amount
  ,tolls_amount
  ,improvement_surcharge
  ,total_amount
  ,congestion_surcharge
FROM
  `dtc-de-352419.nytaxi.yellow_tripdata`;

-- Query scans 1.1 GB of data
SELECT
  COUNT(*) as trips
FROM
  `dtc-de-352419.nytaxi.yellow_tripdata_partitioned`
WHERE
  DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2020-12-31'
  AND VendorId = 1;

-- Query scans 857 MB of data
SELECT
  COUNT(*) as trips
FROM
  `dtc-de-352419.nytaxi.yellow_tripdata_partitioned_clustered`
WHERE
  DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2020-12-31'
  AND VendorId = 1;