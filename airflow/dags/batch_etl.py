import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

# from scripts.extract_load import extract_load
from scripts.extract_source import extract_load
from scripts.transform_data import transform_data
from scripts.convert_to_delta import delta_convert
from scripts.extract_source import download_nyc_taxi_data
from scripts.delta_writer import write_raw_to_bronze
# from scripts.xcom import use_s3_path_from_xcom


tags=[
    "nyc",
    "tripdata",
    "yellow",
    "green",
    "fhv",
    "delta",
    "spark",
]

# Default arguments for the DAG
default_args = {
    "owner": "roger.yau",
    "email_on_failure": False,
    "email_on_retry": False,
    "depends_on_past": False, 
    "email": "admin@localhost.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),    
}

###############################################
# Parameters & Arguments
###############################################
MINIO_ENDPOINT = os.environ['MINIO_ENDPOINT']
MINIO_ACCESS_KEY = os.environ['MINIO_ACCESS_KEY']
MINIO_SECRET_KEY = os.environ['MINIO_SECRET_KEY']
###############################################


with DAG(
    "DEMO.BATCH.JOB.NYCTAXI.v0.0.1",
    start_date=datetime(2014, 12, 2), 
    # end_date=datetime(2024, 5, 1), 
    end_date=datetime(2015, 5, 1), 
    schedule="@monthly", 
    # schedule=None, 
    default_args=default_args,
    # max_active_tasks=4,  # Set the parallelism for the DAG
    max_active_tasks=4,  # Set the parallelism for the DAG
    concurrency=4,
    tags=tags
) as dag:

    # start_pipeline = DummyOperator(
    #     task_id="start_pipeline"
    # )

    # extract_load = PythonOperator(
    #     task_id="extract_load",
    #     python_callable=extract_load,
    #     op_kwargs={'endpoint_url': MINIO_ENDPOINT, 'access_key': MINIO_ACCESS_KEY, 'secret_key': MINIO_SECRET_KEY}
    # )

    # transform_data = PythonOperator(
    #     task_id="transform_data",
    #     python_callable=transform_data,
    #     op_kwargs={'endpoint_url': MINIO_ENDPOINT, 'access_key': MINIO_ACCESS_KEY, 'secret_key': MINIO_SECRET_KEY}
    # )

    # delta_convert = PythonOperator(
    #     task_id="delta_convert",
    #     python_callable=delta_convert,
    #     op_kwargs={'endpoint_url': MINIO_ENDPOINT, 'access_key': MINIO_ACCESS_KEY, 'secret_key': MINIO_SECRET_KEY}
    # )

    download_nyc_taxi_yellow_tripdata_to_raw = PythonOperator(
        provide_context=True,
        task_id="download_nyc_taxi_yellow_tripdata_to_raw",
        python_callable=download_nyc_taxi_data,
        op_kwargs={'endpoint_url': MINIO_ENDPOINT, 'access_key': MINIO_ACCESS_KEY, 'secret_key': MINIO_SECRET_KEY, 'data_entity': "yellow_tripdata", 'bucket_name': "raw"}
    )
        
    # use_s3_path_task_yellow_tripdata = PythonOperator(
    #     task_id="use_s3_path_task_yellow_tripdata",
    #     python_callable=use_s3_path_from_xcom,
    #     provide_context=True,
    #     op_kwargs={'task_ids': 'download_nyc_taxi_yellow_tripdata_to_raw'},
    # )
        
    raw_to_delta_bronze_yellow_tripdata = PythonOperator(
        task_id="raw_to_delta_bronze_yellow_tripdata",
        python_callable=write_raw_to_bronze,
        provide_context=True,
        op_kwargs={
            'endpoint_url': MINIO_ENDPOINT, 
            'access_key': MINIO_ACCESS_KEY, 
            'secret_key': MINIO_SECRET_KEY, 
            'path_write': 's3a://bronze/yellow_tripdata/', 
            'prev_task_ids': 'download_nyc_taxi_yellow_tripdata_to_raw',
            "p_date_column":"tpep_pickup_datetime"
        },
    )
              
        
    download_nyc_taxi_green_tripdata_to_raw = PythonOperator(
        provide_context=True,
        task_id="download_nyc_taxi_green_tripdata_to_raw",
        python_callable=download_nyc_taxi_data,
        op_kwargs={'endpoint_url': MINIO_ENDPOINT, 'access_key': MINIO_ACCESS_KEY, 'secret_key': MINIO_SECRET_KEY, 'data_entity': "green_tripdata", 'bucket_name': "raw"}
    )
    
    # use_s3_path_task_green_tripdata = PythonOperator(
    #     task_id="use_s3_path_task_green_tripdata",
    #     python_callable=use_s3_path_from_xcom,
    #     provide_context=True,
    #     op_kwargs={'task_ids': 'download_nyc_taxi_green_tripdata_to_raw'},
    # )
    
    raw_to_delta_bronze_green_tripdata = PythonOperator(
        task_id="raw_to_delta_bronze_green_tripdata",
        python_callable=write_raw_to_bronze,
        provide_context=True,
        op_kwargs={
            'endpoint_url': MINIO_ENDPOINT, 
            'access_key': MINIO_ACCESS_KEY, 
            'secret_key': MINIO_SECRET_KEY, 
            'path_write': 's3a://bronze/green_tripdata/', 
            'prev_task_ids': 'download_nyc_taxi_green_tripdata_to_raw',
            "p_date_column":"lpep_pickup_datetime"
        },
    )
    
    download_nyc_taxi_fhv_tripdata_to_raw = PythonOperator(
        provide_context=True,
        task_id="download_nyc_taxi_fhv_tripdata_to_raw",
        python_callable=download_nyc_taxi_data,
        op_kwargs={'endpoint_url': MINIO_ENDPOINT, 'access_key': MINIO_ACCESS_KEY, 'secret_key': MINIO_SECRET_KEY, 'data_entity': "fhv_tripdata", 'bucket_name': "raw"}
    )

    # use_s3_path_task_fhv_tripdata = PythonOperator(
    #     task_id="use_s3_path_task_fhv_tripdata",
    #     python_callable=use_s3_path_from_xcom,
    #     provide_context=True,
    #     op_kwargs={'task_ids': 'download_nyc_taxi_fhv_tripdata_to_raw'},
    # )

    # download_nyc_taxi_yellow_tripdata_to_raw >> use_s3_path_task_yellow_tripdata
    # download_nyc_taxi_green_tripdata_to_raw >> use_s3_path_task_green_tripdata
    # download_nyc_taxi_fhv_tripdata_to_raw >> use_s3_path_task_fhv_tripdata
    
    raw_to_delta_bronze_fhv_tripdata = PythonOperator(
        task_id="raw_to_delta_bronze_fhv_tripdata",
        python_callable=write_raw_to_bronze,
        provide_context=True,
        op_kwargs={
            'endpoint_url': MINIO_ENDPOINT, 
            'access_key': MINIO_ACCESS_KEY, 
            'secret_key': MINIO_SECRET_KEY, 
            'path_write': 's3a://bronze/fhv_tripdata/', 
            'prev_task_ids': 'download_nyc_taxi_fhv_tripdata_to_raw',
            "p_date_column":"pickup_datetime"
        },
    )
    
    download_nyc_taxi_yellow_tripdata_to_raw >> raw_to_delta_bronze_yellow_tripdata
    download_nyc_taxi_green_tripdata_to_raw >> raw_to_delta_bronze_green_tripdata
    download_nyc_taxi_fhv_tripdata_to_raw >> raw_to_delta_bronze_fhv_tripdata