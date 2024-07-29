from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
import json
from datetime import datetime, timedelta


default_args = {
    "owner": "roger.yau",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "admin@localhost.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}


connector_config = {
    "name": "taxi-nyc-cdc",
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": "postgresql",
        "database.port": "5432",
        "database.user": "k6",
        "database.password": "k6",
        "database.dbname": "k6",
        "plugin.name": "pgoutput",
        "database.server.name": "device",
        "table.include.list": "iot.taxi_nyc_time_series"
    }
}

tags = [
    "debezium",
    "cdc",
    "postgres",
    "kafka"
]

with DAG(
    'DEMO.DEBEZIUM.OPS.v0.0.1',
    start_date=datetime(2024, 1, 1),
    default_args=default_args,
    schedule_interval=None,
    tags=tags
) as dag:

    create_connector = BashOperator(
        task_id='create_connector',
        bash_command=f"""
        curl -X POST -H "Content-Type: application/json" \
             --data '{json.dumps(connector_config)}' \
             http://streaming-debezium:8083/connectors
        """,
    )