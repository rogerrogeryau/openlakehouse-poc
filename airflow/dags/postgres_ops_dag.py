import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

# from scripts.extract_load import extract_load
# from scripts.transform_data import transform_data
# from scripts.convert_to_delta import delta_convert
# from scripts.convert_to_delta import delta_convert_partition
# from postgresql_client import PostgresSQLClient
from utils.postgresql_client import PostgresSQLClient

# Default arguments for the DAG
default_args = {
    "owner": "roger.yau",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "admin@localhost.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

###############################################
# Parameters & Arguments
###############################################
MINIO_ENDPOINT = os.environ['MINIO_ENDPOINT']
MINIO_ACCESS_KEY = os.environ['MINIO_ACCESS_KEY']
MINIO_SECRET_KEY = os.environ['MINIO_SECRET_KEY']

# POSTGRES_DB = os.environ['POSTGRES_DB']
POSTGRES_DB = "k6"
POSTGRES_USER = "k6"
POSTGRES_PASSWORD = "k6"
###############################################

def create_postgres_schema(postgres_db, postgres_user, postgres_password):
    
    # print(f"postgres_db: {postgres_db}")
    # print(f"postgres_user: {postgres_user}")
    # print(f"postgres_password: {postgres_password}")
    
    pc = PostgresSQLClient(
        database=postgres_db,
        user=postgres_user,
        password=postgres_password,
        host="dw-postgresql",
        port="5432"
    )

    create_iot_schema = """CREATE SCHEMA IF NOT EXISTS iot;"""

    # create_staging_schema = """CREATE SCHEMA IF NOT EXISTS staging;"""

    # create_production_schema = """CREATE SCHEMA IF NOT EXISTS production;"""

    # cli to check in docker 
    # psql -U k6 -d k6
    # \dn

    try:
        pc.execute_query(create_iot_schema)
        # pc.execute_query(create_staging_schema)
        # pc.execute_query(create_production_schema)
    except Exception as e:
        print(f"Failed to create schema with error: {e}")



def create_postgres_tables(postgres_db, postgres_user, postgres_password):
    
    pc = PostgresSQLClient(
        database=postgres_db,
        user=postgres_user,
        password=postgres_password,
        host="dw-postgresql",
        port="5432"
    )

    create_table_iot = """
        CREATE TABLE IF NOT EXISTS iot.taxi_nyc_time_series( 
            VendorID                INT, 
            tpep_pickup_datetime    TIMESTAMP WITHOUT TIME ZONE, 
            tpep_dropoff_datetime   TIMESTAMP WITHOUT TIME ZONE, 
            passenger_count         FLOAT, 
            trip_distance           FLOAT, 
            RatecodeID              FLOAT, 
            store_and_fwd_flag      VARCHAR, 
            PULocationID            INT, 
            DOLocationID            INT, 
            payment_type            INT, 
            fare_amount             FLOAT, 
            extra                   FLOAT, 
            mta_tax                 FLOAT, 
            tip_amount              FLOAT, 
            tolls_amount            FLOAT, 
            improvement_surcharge   FLOAT, 
            total_amount            FLOAT, 
            congestion_surcharge    FLOAT, 
            Airport_fee             FLOAT
        );
    """

    # create_table_staging = """
    #     CREATE TABLE IF NOT EXISTS staging.nyc_taxi (
    #         year                    VARCHAR,
    #         month                   VARCHAR,
    #         dow                     VARCHAR,
    #         vendor_id               INT, 
    #         rate_code_id            FLOAT, 
    #         pickup_location_id      INT, 
    #         dropoff_location_id     INT, 
    #         payment_type_id         INT, 
    #         service_type            INT,
    #         pickup_datetime         TIMESTAMP WITHOUT TIME ZONE, 
    #         dropoff_datetime        TIMESTAMP WITHOUT TIME ZONE, 
    #         pickup_latitude         FLOAT,
    #         pickup_longitude        FLOAT,
    #         dropoff_latitude        FLOAT,
    #         dropoff_longitude       FLOAT,
    #         passenger_count         FLOAT, 
    #         trip_distance           FLOAT,
    #         extra                   FLOAT, 
    #         mta_tax                 FLOAT, 
    #         fare_amount             FLOAT, 
    #         tip_amount              FLOAT, 
    #         tolls_amount            FLOAT, 
    #         total_amount            FLOAT, 
    #         improvement_surcharge   FLOAT, 
    #         congestion_surcharge    FLOAT
    #     );
    # """
    try:
        pc.execute_query(create_table_iot)
        pc.execute_query(create_table_staging)
    except Exception as e:
        print(f"Failed to create table with error: {e}")
    # pass


with DAG("DEMO.POSTGRES.OPS.v0.0.1", start_date=datetime(2024, 1, 1), schedule=None, default_args=default_args) as dag:

    start_pipeline = DummyOperator(
        task_id="start_pipeline"
    )

    create_postgres_schema = PythonOperator(
        task_id="create_postgres_schema",
        python_callable=create_postgres_schema,
        op_kwargs={'postgres_db': POSTGRES_DB, 'postgres_user': POSTGRES_USER, 'postgres_password': POSTGRES_PASSWORD}
    )
    create_postgres_tables = PythonOperator(
        task_id="create_postgres_tables",
        python_callable=create_postgres_tables,
        op_kwargs={'postgres_db': POSTGRES_DB, 'postgres_user': POSTGRES_USER, 'postgres_password': POSTGRES_PASSWORD}
    )

    start_pipeline >> create_postgres_schema >> create_postgres_tables