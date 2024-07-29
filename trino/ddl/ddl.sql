-- create schema
CREATE SCHEMA IF NOT EXISTS datalake.bronze WITH (location = 's3a://bronze/');

-- yellow_tripdata
DROP TABLE IF EXISTS datalake.bronze.yellow_tripdata;

CREATE TABLE datalake.bronze.yellow_tripdata (
       VendorID INT,
       tpep_pickup_datetime TIMESTAMP,
       tpep_dropoff_datetime TIMESTAMP,
       passenger_count INT,
       trip_distance DOUBLE,
       RatecodeID INT,
       store_and_fwd_flag VARCHAR,
       PULocationID INT,
       DOLocationID INT,
       payment_type INT,
       fare_amount DOUBLE,
       extra DOUBLE,
       mta_tax DOUBLE,
       tip_amount DOUBLE,
       tolls_amount DOUBLE,
       improvement_surcharge DOUBLE,
       total_amount DOUBLE,
       congestion_surcharge INT,
       airport_fee INT,
       p_date VARCHAR
)
WITH (
    format = 'PARQUET',
    external_location = 's3a://bronze/yellow_tripdata/',
    partitioned_by = ARRAY['p_date']
);

CALL datalake.system.sync_partition_metadata('bronze', 'yellow_tripdata', 'ADD');

-- SELECT * FROM datalake.bronze.yellow_tripdata;



-- green_tripdata
DROP TABLE IF EXISTS datalake.bronze.green_tripdata;

CREATE TABLE datalake.bronze.green_tripdata (
       VendorID INT,
       lpep_pickup_datetime TIMESTAMP,
       lpep_dropoff_datetime TIMESTAMP,
       store_and_fwd_flag VARCHAR,
       RatecodeID INT,
       PULocationID INT,
       DOLocationID INT,
       passenger_count INT,
       trip_distance DOUBLE,
       fare_amount DOUBLE,
       extra DOUBLE,
       mta_tax DOUBLE,
       tip_amount DOUBLE,
       tolls_amount DOUBLE,
       ehail_fee INT,
       improvement_surcharge DOUBLE,
       total_amount DOUBLE,
       payment_type INT,
       trip_type DOUBLE,
       congestion_surcharge INT,
       p_date VARCHAR
)
WITH (
    format = 'PARQUET',
    external_location = 's3a://bronze/green_tripdata/',
    partitioned_by = ARRAY['p_date']
);

call datalake.system.sync_partition_metadata('bronze', 'green_tripdata', 'ADD');


-- select * from datalake.bronze.green_tripdata;



-- fhv_tripdata
DROP TABLE IF EXISTS datalake.bronze.fhv_tripdata;

CREATE TABLE datalake.bronze.fhv_tripdata (
       dispatching_base_num VARCHAR,
       pickup_datetime TIMESTAMP,
       dropOff_datetime TIMESTAMP,
       PUlocationID DOUBLE,
       DOlocationID DOUBLE,
       SR_Flag INT,
       Affiliated_base_number VARCHAR,
       p_date VARCHAR
)
WITH (
    format = 'PARQUET',
    external_location = 's3a://bronze/fhv_tripdata/',
    partitioned_by = ARRAY['p_date']
);

CALL datalake.system.sync_partition_metadata('bronze', 'fhv_tripdata', 'ADD');

-- SELECT * FROM datalake.bronze.fhv_tripdata;

-- fake_device_iot_taxi_nyc_time_series
DROP TABLE IF EXISTS datalake.bronze.fake_device_iot_taxi_nyc_time_series;

CREATE TABLE datalake.bronze.fake_device_iot_taxi_nyc_time_series (
       dolocationid INT,
       pulocationid INT,
       ratecodeid DOUBLE,
       vendorid INT,
       congestion_surcharge DOUBLE,
       extra DOUBLE,
       fare_amount DOUBLE,
       improvement_surcharge DOUBLE,
       mta_tax DOUBLE,
       passenger_count DOUBLE,
       payment_type INT,
       tip_amount DOUBLE,
       tolls_amount DOUBLE,
       total_amount DOUBLE,
       tpep_dropoff_datetime TIMESTAMP,
       tpep_pickup_datetime TIMESTAMP,
       trip_distance DOUBLE,
       p_date VARCHAR
)
WITH (
    format = 'PARQUET',
    external_location = 's3a://bronze/fake_device_iot_taxi_nyc_time_series/',
    partitioned_by = ARRAY['p_date']
);

CALL datalake.system.sync_partition_metadata('bronze', 'fake_device_iot_taxi_nyc_time_series', 'ADD');

-- SELECT * FROM datalake.bronze.fake_device_iot_taxi_nyc_time_series;
