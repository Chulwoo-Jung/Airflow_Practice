import pendulum

from airflow.sdk import DAG, task

with DAG(
    dag_id="dags_python_task_decorator",
    schedule="30 6 * * 1",  # every sunday at 6:30 AM
    start_date=pendulum.datetime(2025, 1, 1, tz="Europe/Berlin"),
    catchup=False,
) as dag:
    
    @task(task_id="python_task_1")
    def print_context(input):
        print(input)

    python_task_1 = print_context('task_decorator 실행')