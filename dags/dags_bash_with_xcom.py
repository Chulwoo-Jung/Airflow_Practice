from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
import pendulum

with DAG(
    dag_id='dags_bash_with_xcom',
    schedule='30 6 * * *',
    start_date=pendulum.datetime(2025, 6, 1, tz='Europe/Berlin'),
    catchup=False
) as dag:
    
    bash_push = BashOperator(
        task_id='bash_push',
        bash_command='echo START &&'
                    'echo XCOM_PUSHED'
                    '{{ ti.xcom_push(key="bash_pushed", value="first_bash_message")}}'
                    'echo COMPLETED'
    )
    
    bash_pull = BashOperator(
        task_id='bash_pull',
        env = {'PUSHED_VALUE' : '{{ ti.xcom_pull(key="bash_pushed", task_ids="bash_push")}}',
               'RETURN_VALUE' : '{{ ti.xcom_pull(key="return_value", task_ids="bash_push")}}'},
        bash_command='echo "PUSHED_VALUE: $PUSHED_VALUE"'
                    'echo "RETURN_VALUE: $RETURN_VALUE"',
        do_xcom_push = False
    )
    
    bash_push >> bash_pull