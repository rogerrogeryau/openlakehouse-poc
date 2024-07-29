
VendorID                       VARCHAR
lpep_pickup_datetime           VARCHAR
lpep_dropoff_datetime          VARCHAR
store_and_fwd_flag             VARCHAR
RatecodeID                     VARCHAR
PULocationID                   VARCHAR
DOLocationID                   VARCHAR
passenger_count                VARCHAR
trip_distance                  VARCHAR
fare_amount                    VARCHAR
extra                          VARCHAR
mta_tax                        VARCHAR
tip_amount                     VARCHAR
tolls_amount                   VARCHAR
ehail_fee                      VARCHAR
improvement_surcharge          VARCHAR
total_amount                   VARCHAR
payment_type                   VARCHAR
trip_type                      VARCHAR
congestion_surcharge           VARCHAR
p_date                         VARCHAR



CREATE SCHEMA IF NOT EXISTS datalake.bronze WITH (location = 's3a://bronze/');

SHOW TABLES FROM datalake.bronze;

CREATE TABLE IF NOT EXISTS datalake.bronze.green_tripdata_2(
    VendorID                       VARCHAR,
    lpep_pickup_datetime           VARCHAR,
    lpep_dropoff_datetime          VARCHAR,
    store_and_fwd_flag             VARCHAR,
    RatecodeID                     VARCHAR,
    PULocationID                   VARCHAR,
    DOLocationID                   VARCHAR,
    passenger_count                VARCHAR,
    trip_distance                  VARCHAR,
    fare_amount                    VARCHAR,
    extra                          VARCHAR,
    mta_tax                        VARCHAR,
    tip_amount                     VARCHAR,
    tolls_amount                   VARCHAR,
    ehail_fee                      VARCHAR,
    improvement_surcharge          VARCHAR,
    total_amount                   VARCHAR,
    payment_type                   VARCHAR,
    trip_type                      VARCHAR,
    congestion_surcharge           VARCHAR,
    p_date                         VARCHAR
    
) WITH (
    external_location = 's3a://bronze/green_tripdata/',
    format = 'PARQUET'
);

CREATE TABLE IF NOT EXISTS datalake.raw.green_tripdata_2(
    VendorID                       VARCHAR,
    lpep_pickup_datetime           VARCHAR,
    lpep_dropoff_datetime          VARCHAR,
    store_and_fwd_flag             VARCHAR,
    RatecodeID                     VARCHAR,
    PULocationID                   VARCHAR,
    DOLocationID                   VARCHAR,
    passenger_count                VARCHAR,
    trip_distance                  VARCHAR,
    fare_amount                    VARCHAR,
    extra                          VARCHAR,
    mta_tax                        VARCHAR,
    tip_amount                     VARCHAR,
    tolls_amount                   VARCHAR,
    ehail_fee                      VARCHAR,
    improvement_surcharge          VARCHAR,
    total_amount                   VARCHAR,
    payment_type                   VARCHAR,
    trip_type                      VARCHAR,
    congestion_surcharge           VARCHAR,
    p_date                         VARCHAR
    
) WITH (
    external_location = 's3a://raw/green_tripdata/',
    format = 'PARQUET'
);



CREATE TABLE IF NOT EXISTS datalake.bronze.green_tripdata_p(
    VendorID                       VARCHAR,
    lpep_pickup_datetime           VARCHAR,
    lpep_dropoff_datetime          VARCHAR,
    store_and_fwd_flag             VARCHAR,
    RatecodeID                     VARCHAR,
    PULocationID                   VARCHAR,
    DOLocationID                   VARCHAR,
    passenger_count                VARCHAR,
    trip_distance                  VARCHAR,
    fare_amount                    VARCHAR,
    extra                          VARCHAR,
    mta_tax                        VARCHAR,
    tip_amount                     VARCHAR,
    tolls_amount                   VARCHAR,
    ehail_fee                      VARCHAR,
    improvement_surcharge          VARCHAR,
    total_amount                   VARCHAR,
    payment_type                   VARCHAR,
    trip_type                      VARCHAR,
    congestion_surcharge           VARCHAR,
    p_date           VARCHAR
)
WITH (
    external_location = 's3a://bronze/green_tripdata',
    format = 'PARQUET',
    partitioned_by = ARRAY['p_date']
);



-- ###
CREATE SCHEMA IF NOT EXISTS datalake.bronze WITH (location = 's3a://bronze/');


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
    congestion_surcharge INTEGER,
    airport_fee INTEGER,
    p_date VARCHAR
)
WITH (
    external_location = 's3a://bronze/yellow_tripdata/',
    format = 'PARQUET',
    partitioned_by = ARRAY['p_date']
);



CALL datalake.system.sync_partition_metadata('datalake', 'default', 'yellow_tripdata', 'ADD');
CALL datalake.system.sync_partition_metadata('datalake', 'default', 'yellow_tripdata', 'ADD');
CALL datalake.system.sync_partition_metadata('test_lake', 'green_taxi_p_2', 'ADD');



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
       trip_type INT,
       congestion_surcharge DOUBLE,
       p_date VARCHAR
)
WITH (
    format = 'PARQUET',
    external_location = 's3a://bronze/green_tripdata/',
    partitioned_by = ARRAY['p_date']
);
CALL datalake.system.sync_partition_metadata('datalake', 'default' ,'green_tripdata', 'ADD');




DROP TABLE IF EXISTS datalake.bronze.green_tripdata;

SHOW TABLES FROM datalake.bronze;


# try 
call datalake.system.sync_partition_metadata('bronze', 'green_tripdata', 'ADD');
-- CALL datalake.system.sync_partition_metadata('test_lake', 'green_taxi_p_2', 'ADD');