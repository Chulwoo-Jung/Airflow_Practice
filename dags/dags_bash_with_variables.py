from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
import pendulum

with DAG(
    dag_id='dags_bash_with_variables',
    schedule='30 6 * * *',
    start_date=pendulum.datetime(2025, 6, 1, tz='Europe/Berlin'),
    catchup=False
) as dag:
    
    bash_task = BashOperator(
        task_id='bash_task',
        bash_command = 'echo "{{ var.value.sample_key }}"'
    )

    bash_task