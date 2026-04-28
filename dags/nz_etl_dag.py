from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os

sys.path.append("/opt/airflow")

from etl.weather_etl import main as weather_etl
from etl.transport_etl import main as transport_etl


default_args = {
    "owner": "data_engineer",
    "start_date": datetime(2024, 1, 1),
    "retries": 1
}

dag = DAG(
    "nz_transport_weather_pipeline",
    default_args=default_args,
    schedule_interval="*/10 * * * *",  # every 10 minutes
    catchup=False
)


weather_task = PythonOperator(
    task_id="weather_etl",
    python_callable=weather_etl,
    dag=dag
)

transport_task = PythonOperator(
    task_id="transport_etl",
    python_callable=transport_etl,
    dag=dag
)

weather_task >> transport_task
