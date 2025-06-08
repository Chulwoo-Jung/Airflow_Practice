from airflow.sdk import DAG
import pendulum
import datetime as dt
from airflow.providers.standard.operators.python import PythonOperator
from common.common_func import get_stfp

with DAG(
    dag_id = 'dags_python_import_func',
    schedule = "30 6 * * *",
    start_date = pendulum.datetime(2025,1,1, tz="Europe/Berlin"),
    catchup = False
) as dag:
    
    task_get_stfp = PythonOperator(
        task_id = 'task_get_stfp',
        python_callable = get_stfp
    )

    task_get_stfp