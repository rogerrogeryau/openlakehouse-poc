from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os
import psycopg2
from faker import Faker
from time import sleep
import random

# Database credentials
DB_NAME = "k6"
DB_USER = "k6"
DB_PASSWORD = "k6"
DB_HOST = "dw-postgresql"
DB_PORT = "5432"
TABLE_NAME = "iot.taxi_nyc_time_series"
NUM_ROWS = 1000

default_args = {
    'owner': 'roger.yau',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

fake = Faker()

def generate_synthetic_data(num_rows):
    data = []
    for _ in range(num_rows):
        row = {
            "vendorid": fake.random_int(min=1, max=1000000),
            "tpep_pickup_datetime": fake.date_time_this_decade(),
            "tpep_dropoff_datetime": fake.date_time_this_decade(),
            "passenger_count": random.uniform(1, 6),
            "trip_distance": random.uniform(0, 50),
            "ratecodeid": random.uniform(1, 6),
            "store_and_fwd_flag": fake.random_element(elements=("Y", "N")),
            "pulocationid": fake.random_int(min=1, max=265),
            "dolocationid": fake.random_int(min=1, max=265),
            "payment_type": fake.random_int(min=1, max=5),
            "fare_amount": random.uniform(2.5, 100),
            "extra": random.uniform(0, 10),
            "mta_tax": random.uniform(0, 0.5),
            "tip_amount": random.uniform(0, 30),
            "tolls_amount": random.uniform(0, 20),
            "improvement_surcharge": random.uniform(0, 0.5),
            "total_amount": random.uniform(2.5, 200),
            "congestion_surcharge": random.uniform(0, 2.75),
            "airport_fee": random.uniform(0, 5)
            
        }
        data.append(row)
    return data

def insert_data_to_postgres():
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    cur = conn.cursor()

    # Fetch columns from the table
    columns = [
        'vendorid', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count', 
        'trip_distance', 'ratecodeid', 'store_and_fwd_flag', 'pulocationid', 
        'dolocationid', 'payment_type', 'fare_amount', 'extra', 'mta_tax', 
        'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount', 
        'congestion_surcharge', 'airport_fee'
    ]

    data = generate_synthetic_data(NUM_ROWS)
    print(f"synthetic data count : {len(data)}")    
    
    for row in data:
        values = tuple(row[col] for col in columns)
        insert_query = f"""
            INSERT INTO {TABLE_NAME} ({', '.join(columns)}) 
            VALUES ({', '.join(['%s'] * len(columns))})
        """
        cur.execute(insert_query, values)

    conn.commit()
    cur.close()
    conn.close()


tags = [
    "fake_live_data",
    "postgres",
    "cdc"
]

with DAG(
    'DEMO.FAKELIVEDATA.POSTGRES.v.0.0.1', 
    start_date=datetime(2024, 1, 1), 
    schedule=None,
    default_args=default_args,
    tags=tags
) as dag:

    insert_data = PythonOperator(
        task_id='insert_data',
        python_callable=insert_data_to_postgres,
        dag=dag,
    )

    insert_data
