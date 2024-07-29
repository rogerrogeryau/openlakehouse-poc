# import os
# from datetime import datetime, timedelta

# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.operators.dummy import DummyOperator

# # from scripts.extract_load import extract_load
# from scripts.extract_source import extract_load
# from scripts.transform_data import transform_data
# from scripts.convert_to_delta import delta_convert
# from scripts.delta_reader import debug_reader
# from scripts.extract_source import download_nyc_taxi_data

# # Default arguments for the DAG
# default_args = {
#     "owner": "sleekflow_user",
#     "email_on_failure": False,
#     "email_on_retry": False,
#     "email": "admin@localhost.com",
#     "retries": 1,
#     "retry_delay": timedelta(minutes=5),
    
# }

# ###############################################
# # Parameters & Arguments
# ###############################################
# MINIO_ENDPOINT = os.environ['MINIO_ENDPOINT']
# MINIO_ACCESS_KEY = os.environ['MINIO_ACCESS_KEY']
# MINIO_SECRET_KEY = os.environ['MINIO_SECRET_KEY']
# ###############################################


# with DAG(
#     "delta_debug_reader_2",
#     start_date=datetime(2014, 12, 1), 
#     # end_date=datetime(2024, 5, 1), 
#     # schedule="@monthly", 
#     schedule=None, 
#     default_args=default_args,
#     max_active_tasks=3  # Set the parallelism for the DAG
# ) as dag:


#     # delta_convert = PythonOperator(
#     #     task_id="delta_convert",
#     #     python_callable=delta_convert,
#     #     op_kwargs={'endpoint_url': MINIO_ENDPOINT, 'access_key': MINIO_ACCESS_KEY, 'secret_key': MINIO_SECRET_KEY}
#     # )
    
#     debug_reader = PythonOperator(
#         task_id="debug_reader",
#         python_callable=debug_reader,
#         op_kwargs={'endpoint_url': MINIO_ENDPOINT, 'access_key': MINIO_ACCESS_KEY, 'secret_key': MINIO_SECRET_KEY}
#     )

#     debug_reader

#     # start_pipeline = DummyOperator(
#     #     task_id="start_pipeline"
#     # )

#     # extract_load = PythonOperator(
#     #     task_id="extract_load",
#     #     python_callable=extract_load,
#     #     op_kwargs={'endpoint_url': MINIO_ENDPOINT, 'access_key': MINIO_ACCESS_KEY, 'secret_key': MINIO_SECRET_KEY}
#     # )

#     # transform_data = PythonOperator(
#     #     task_id="transform_data",
#     #     python_callable=transform_data,
#     #     op_kwargs={'endpoint_url': MINIO_ENDPOINT, 'access_key': MINIO_ACCESS_KEY, 'secret_key': MINIO_SECRET_KEY}
#     # )

#     # delta_convert = PythonOperator(
#     #     task_id="delta_convert",
#     #     python_callable=delta_convert,
#     #     op_kwargs={'endpoint_url': MINIO_ENDPOINT, 'access_key': MINIO_ACCESS_KEY, 'secret_key': MINIO_SECRET_KEY}
#     # )

#     # download_nyc_taxi_yellow_tripdata_to_bronze = PythonOperator(
#     #     provide_context=True,
#     #     task_id="download_nyc_taxi_yellow_tripdata_to_bronze",
#     #     python_callable=download_nyc_taxi_data,
#     #     op_kwargs={'endpoint_url': MINIO_ENDPOINT, 'access_key': MINIO_ACCESS_KEY, 'secret_key': MINIO_SECRET_KEY, 'data_entity': "yellow_tripdata"}
#     # )
    
#     # download_nyc_taxi_green_tripdata_to_bronze = PythonOperator(
#     #     provide_context=True,
#     #     task_id="download_nyc_taxi_green_tripdata_to_bronze",
#     #     python_callable=download_nyc_taxi_data,
#     #     op_kwargs={'endpoint_url': MINIO_ENDPOINT, 'access_key': MINIO_ACCESS_KEY, 'secret_key': MINIO_SECRET_KEY, 'data_entity': "green_tripdata"}
#     # )
    
#     # download_nyc_taxi_fhv_tripdata_to_bronze = PythonOperator(
#     #     provide_context=True,
#     #     task_id="download_nyc_taxi_fhv_tripdata_to_bronze",
#     #     python_callable=download_nyc_taxi_data,
#     #     op_kwargs={'endpoint_url': MINIO_ENDPOINT, 'access_key': MINIO_ACCESS_KEY, 'secret_key': MINIO_SECRET_KEY, 'data_entity': "fhv_tripdata"}
#     # )

#     # end_pipeline = DummyOperator(
#     #     task_id="end_pipeline"
#     # )

# # start_pipeline >> extract_load >> transform_data >> delta_convert >> end_pipeline
#     # download_nyc_taxi_data
