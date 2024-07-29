import os
from datetime import datetime, timedelta
from utils.minio_utils import MinIOClient

# minio_utils import MinIOClient


from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

# from scripts.extract_load import extract_load
# from scripts.transform_data import transform_data
# from scripts.convert_to_delta import delta_convert
# from scripts.convert_to_delta import delta_convert_partition
from scripts.spark_stream import consume_kafka_topic


# from pyspark.sql import SparkSession
# from pyspark.sql.functions import from_json, col
# from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, LongType


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
###############################################


tags = [
    "kafka",
    "consumer",
    "minio",
    "spark",
    "streaming",
]

with DAG(
    "DEMO.KAFKA.CONSUMER.v0.0.1", 
    start_date=datetime(2024, 1, 1), 
    schedule=None, 
    default_args=default_args,
    tags=tags
) as dag:

    start_pipeline = DummyOperator(
        task_id="start_pipeline"
    )

    consume_kafka_topic = PythonOperator(
        task_id="consume_kafka_topic",
        python_callable=consume_kafka_topic,
        op_kwargs={'endpoint_url': MINIO_ENDPOINT, 'access_key': MINIO_ACCESS_KEY, 'secret_key': MINIO_SECRET_KEY}
    )
