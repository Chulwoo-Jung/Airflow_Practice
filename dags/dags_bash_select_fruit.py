from airflow.sdk import DAG
import pendulum
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    dag_id = 'dags_bash_select_fruit',
    schedule = "10 0 * * 6#1",
    start_date = pendulum.datetime(2025,1,1, tz="Europe/Berlin"),
    catchup = False
) as dag:
    t1_orange = BashOperator(
        task_id = 't1_orange',
        bash_command = '/opt/airflow/plugins/shell/select_fruit.sh ORANGE'
    )

    t2_avocado = BashOperator(
        task_id = 't2_avocado',
        bash_command = '/opt/airflow/plugins/shell/select_fruit.sh AVOCADO'
    )

    t3_grape = BashOperator(
        task_id = 't3_grape',
        bash_command = '/opt/airflow/plugins/shell/select_fruit.sh GRAPE'
    )

    t1_orange >> [t2_avocado, t3_grape]


    
    
    
    