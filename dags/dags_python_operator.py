from airflow.sdk import DAG
import pendulum
import datetime as dt
from airflow.providers.standard.operators.python import PythonOperator
import random

with DAG(
    dag_id = 'dags_python_operator',
    schedule = "30 6 * * *",
    start_date = pendulum.datetime(2025,1,1, tz="Europe/Berlin"),
    catchup = False
) as dag:
    
    def select_fruit():
        fruit = ['APPLE', 'BANANA', 'ORANGE', 'AVOCADO']
        rand_int = random.randint(0,3)
        return fruit[rand_int]
    
    python_t1 = PythonOperator(
        task_id = 'python_t1',
        python_callable = select_fruit
    )

    python_t2 = PythonOperator(
        task_id = 'python_t2',
        python_callable = select_fruit
    )

    python_t1 >> python_t2